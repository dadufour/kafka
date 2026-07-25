/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.raft

import java.util.{Optional, OptionalInt, Map => JMap}
import kafka.server.KafkaConfig
import kafka.utils.Logging
import org.apache.kafka.clients.{ApiVersions, ManualMetadataUpdater, MetadataRecoveryStrategy, NetworkClient}
import org.apache.kafka.common.Uuid
// import org.apache.kafka.common.metadata.ObserverReplicationOffsetRecord;
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ChannelBuilders, ListenerName, NetworkReceive, Selectable, Selector}
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.utils.internals.LogContext
import org.apache.kafka.image.publisher.MetadataPublisher
import org.apache.kafka.image.loader.LoaderManifest
import org.apache.kafka.image.{MetadataDelta, MetadataImage}
import org.apache.kafka.raft.internals.EphemeralRaftLog
import org.apache.kafka.raft.{Endpoints, ExternalKRaftMetrics, VoidQuorumStateStore, KafkaNetworkChannel, KafkaRaftClient, KafkaRaftClientDriver, QuorumConfig, TimingWheelExpirationService, RaftClient, BatchReader, LeaderAndEpoch, KRaftConfigs}
import org.apache.kafka.server.config.{ReplicationConfigs, ServerConfigs}
import org.apache.kafka.server.ProcessRole
// import org.apache.kafka.server.common.ApiMessageAndVersion
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.server.common.serialization.RecordSerde
import org.apache.kafka.server.common.Feature
import org.apache.kafka.server.config.ClusterLinkConfigs
import org.apache.kafka.server.fault.ProcessTerminatingFaultHandler
import org.apache.kafka.server.util.timer.SystemTimer
import org.apache.kafka.snapshot.SnapshotReader

import scala.jdk.CollectionConverters._

//=============================================================
// The KafkaRaftObserver is a bridge between 2 different Kraft
// metadata log.
// It is part of KRaft Controllers.
// It consumes the metadata of a DISTINCT KRaft cluster and
// filters/replicates/transforms metadata events into the local
// KRaft cluster.
// It is active only when:
//   - the local cluster has Linking activated
//   - the hosting controller is Leader
// Therefore this class is activated/deactivated according to
// the leader role of the hosting controller.
//=============================================================
object KafkaRaftObserver {

  // The KafkaRaftObserver is built on the ability of the KafkaRaftClient
  // to be used from Brokers as simple metadata consumer.
  // We need to tweak the Controller config to make it suitable for this purpose.
  private def observerConfigFromControllerConfig(controllerConfig: KafkaConfig): KafkaConfig = {
  
    // Change the logdir and nodeId for observer 
    val origProps = controllerConfig.originals()
    origProps.put(ServerConfigs.BROKER_ID_CONFIG, f"${controllerConfig.clusterLinkConfig.observerNodeId}")    // Override Broker/Node Ids
    origProps.put(KRaftConfigs.NODE_ID_CONFIG, f"${controllerConfig.clusterLinkConfig.observerNodeId}")       // Override Broker/Node Ids
    origProps.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, controllerConfig.clusterLinkConfig.listenerNames) // Override listener
    origProps.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")                                                // Must behave as an observer (=broker)
    origProps.put("controller.quorum.bootstrap.servers", controllerConfig.clusterLinkConfig.sourceQuorumBootstrapServers)
    origProps.put(SocketServerConfigs.LISTENERS_CONFIG, "DUMMY://:9999")                                      // Not used but required for consistency checks!
    origProps.put(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, "DUMMY://nowhere:9999")                    // Not used but required for consistency checks!
    origProps.put(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, "DUMMY")                              // Not used but required for consistency checks!
    origProps.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, 
                  origProps.get(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG)+",DUMMY:PLAINTEXT") // Required for consistency checks!
    KafkaConfig.apply(origProps, false)
  }
}
//=============================================================

//=============================================================
class KafkaRaftObserver[T](
  private val _config: KafkaConfig,
  serde: RecordSerde[T],
  time: Time,
  metrics: Metrics
) extends Logging {

  private val config = KafkaRaftObserver.observerConfigFromControllerConfig(_config)
  val sourceClusterId = config.clusterLinkConfig.sourceClusterId
  val sourceBootstrapServers = ClusterLinkConfigs.parseBootstrapServers(config.clusterLinkConfig.sourceQuorumBootstrapServers)
  private val observerReplicationOffsetPublisher = new ObserverReplicationOffsetPublisher()
  val replicationOffsetPublisher: MetadataPublisher = observerReplicationOffsetPublisher
  private var observerReplicationOffsetCache: Long = 0L
  private val raftConfig = new QuorumConfig(config)
  private val threadNamePrefix = s"kafka-observer"
  private val logContext = new LogContext(s"[RaftObserver id=${config.nodeId}] ")
  this.logIdent = logContext.logPrefix()


  //=============================================================
  // Everything from the Observer that needs to get built each time
  // the Observer is started. The Observer should be active ONLY when
  // the associated local Controller is LEADER. So each time the
  // leadership changes, the Observer needs to be stopped/started.
  //=============================================================
  private class RunningObserver() {
  
    private val raftLog = buildMetadataLog(KafkaRaftObserver.this.observerReplicationOffsetCache)
    private val netChannel = buildNetworkChannel()
    private val client: KafkaRaftClient[T] = buildRaftClient()
    private val clientDriver = new KafkaRaftClientDriver[T](client, 
            threadNamePrefix, 
            new ProcessTerminatingFaultHandler.Builder().build(), 
            logContext)

    private def buildMetadataLog(observerReplicationOffset: Long): EphemeralRaftLog = {
      EphemeralRaftLog.createLog(
        config.nodeId,
        observerReplicationOffset
      )
    }

    private def buildRaftClient(): KafkaRaftClient[T] = {
      val client = new KafkaRaftClient(
        OptionalInt.of(config.nodeId),
        Uuid.randomUuid(),            // There is no physical Metadata Log Dir (because of EphemeralRaftLog) so we provide a random Uuid 
        serde,
        netChannel,
        raftLog,
        time,
        expirationService,
        logContext,
        // Controllers should always flush the log on replication because they may become voters
        config.processRoles.contains(ProcessRole.ControllerRole),
        sourceClusterId,
        sourceBootstrapServers,
        Endpoints.empty(),									// localListeners: Not used
        Feature.KRAFT_VERSION.supportedVersionRange(),
        raftConfig
      )
      client.register(new RaftObserverListener)
      client
    }
    
    private def buildNetworkChannel(): KafkaNetworkChannel = {
      val (listenerName, netClient) = buildNetworkClient()
      new KafkaNetworkChannel(time, listenerName, netClient, config.quorumConfig.requestTimeoutMs, threadNamePrefix)
    }

    private def buildNetworkClient(): (ListenerName, NetworkClient) = {
      val controllerListenerName = new ListenerName(config.controllerListenerNames.get(0))
      val controllerSecurityProtocol = Option(config.effectiveListenerSecurityProtocolMap.get(controllerListenerName))
        .getOrElse(SecurityProtocol.forName(controllerListenerName.value()))
      val channelBuilder = ChannelBuilders.clientChannelBuilder(
        controllerSecurityProtocol,
        JaasContext.Type.SERVER,
        config,
        controllerListenerName,
        config.saslMechanismControllerProtocol,
        time,
        logContext
      )

      val metricGroupPrefix = "observer-channel"
      val collectPerConnectionMetrics = false

      val selector = new Selector(
        NetworkReceive.UNLIMITED,
        config.connectionsMaxIdleMs,
        metrics,
        time,
        metricGroupPrefix,
        Map.empty[String, String].asJava,
        collectPerConnectionMetrics,
        channelBuilder,
        logContext
      )

      val clientId = s"raft-client-${config.nodeId}"
      val maxInflightRequestsPerConnection = 1
      val reconnectBackoffMs = 50
      val reconnectBackoffMsMs = 500
      val discoverBrokerVersions = true

      val networkClient = new NetworkClient(
        selector,
        new ManualMetadataUpdater(),
        clientId,
        maxInflightRequestsPerConnection,
        reconnectBackoffMs,
        reconnectBackoffMsMs,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        config.socketReceiveBufferBytes,
        config.quorumConfig.requestTimeoutMs,
        config.connectionSetupTimeoutMs,
        config.connectionSetupTimeoutMaxMs,
        time,
        discoverBrokerVersions,
        new ApiVersions(),
        logContext,
        MetadataRecoveryStrategy.NONE
      )

      (controllerListenerName, networkClient)
    }
    
    def start(): Unit = {
      client.initialize(
          JMap.of(),					// Not used: controllerQuorumVoters
          new VoidQuorumStateStore(),	// Not used: void implementation
          metrics,
          new ExternalKRaftMetrics {    // Not used: void implementation
            override def setIgnoredStaticVoters(ignoredStaticVoters: Boolean): Unit = ()
          },
          s"observer"
      )
      netChannel.start()
      clientDriver.start()
    }
    
    def stop(): Unit = {
      clientDriver.shutdown()
      netChannel.close()
      raftLog.close()
    }
    
  }
  //=============================================================

  private var runningObserver: RunningObserver = _
  private val expirationTimer = new SystemTimer("raft-expiration-executor")
  private val expirationService = new TimingWheelExpirationService(expirationTimer)

  private def startup(): Unit = {
    if (runningObserver == null) {
      runningObserver = new RunningObserver
      runningObserver.start()
    }
  }

  private def stop(): Unit = {
    if (runningObserver != null) {
      runningObserver.stop()
      runningObserver = null
    }
  }
  
  private def isRunning(): Boolean = {
    (runningObserver != null)
  }

  def shutdown(): Unit = {
    logger.info("Shutting down Observer")
    stop()
    Utils.swallow(this.logger.underlying, () => expirationService.shutdown())
    Utils.closeQuietly(expirationTimer, "expiration timer")
  }

  def buildStarter[U](raftClient: RaftClient[U],
    				  controllerId: Int
    				  ): Unit = {
    	// The Listener on the remote cluster can't be started immediately: 
    	// to start from the correct replication offset, the Observer should 
    	// first have a MetadataListener installed on the local metadata log 
    	// of the local controller to know that offset. 
    	// If this Listener is installed too early, the Observer
    	// may start before this replication offset is known. As a consequence,
    	// the installation of this listener will be deferred up to the point
    	// we have received at least once the metadata log of the local controller
    	observerReplicationOffsetPublisher.installLeadershipListenerAction = () => { raftClient.register(new RaftLeadershipListener[U](controllerId)) }
  }
  
  //=============================================================
  // A Client Listener that listens to the local leader controller
  // One instance will be running constantly to detect change of
  // leadership in the local quorum to start/stop the observer
  // accordingly
  //=============================================================
  private class RaftLeadershipListener[U](
    val controllerId: Int
  ) extends RaftClient.Listener[U] {

    override def handleLeaderChange(newLeaderAndEpoch: LeaderAndEpoch): Unit = {
      if (newLeaderAndEpoch.isLeader(controllerId)) {
        if (KafkaRaftObserver.this.isRunning() == false) {
            KafkaRaftObserver.this.logger.info("Starting RaftObserver because controller {} became leader", controllerId)
            KafkaRaftObserver.this.logger.info("Replication offset is {}", KafkaRaftObserver.this.observerReplicationOffsetCache)

        	KafkaRaftObserver.this.startup()
        }
      } else {
        if (KafkaRaftObserver.this.isRunning() == true) {
            KafkaRaftObserver.this.logger.info("Stopping RaftObserver because controller {} lost leadership", controllerId)
        	KafkaRaftObserver.this.stop()
        }
      }
    }

    override def handleCommit(reader: BatchReader[U]): Unit = {
    	reader.close();
    }

    override def handleLoadSnapshot(reader: SnapshotReader[U]): Unit = {
    	reader.close()
    }

    override def handleLoadBootstrap(reader: SnapshotReader[U]): Unit = {
    	reader.close()
    }
  }
  //=============================================================
  
  //=============================================================
  // A Client Listener that listens to the remote leader controller
  // A new instance will be created each time the observer is
  // (re)started.
  //=============================================================
  private class RaftObserverListener(
  ) extends RaftClient.Listener[T] {

    override def handleLeaderChange(newLeaderAndEpoch: LeaderAndEpoch): Unit = {
    }

    private def translate(inputRecord: T): Optional[T] = {
        return Optional.empty()
    }
    
    override def handleCommit(reader: BatchReader[T]): Unit = {
    
      	var lastOffset: Long = -1;
      	
      	try {
        	while (reader.hasNext()) {
            	val batch = reader.next();
            	lastOffset = batch.lastOffset();
                batch.forEach { record =>
                  val localRecord = translate(record)
                  if (localRecord.isPresent) {
                    KafkaRaftObserver.this.logger.info("present")
                  }
                }
        	}
      	} finally {
        	reader.close();
      	}
      	
      	if (lastOffset >= 0) {
        	KafkaRaftObserver.this.logger.info("new replication offset {}", lastOffset)
        	// buildOffsetRecord(lastOffset)
      	}
    }

/*
    private def buildOffsetRecord(offset: Long): ApiMessageAndVersion = {
        new ApiMessageAndVersion(
            new ObserverReplicationOffsetRecord()
                .setReplicationOffset(offset)
                .setPrimaryClusterId(Uuid.fromString(sourceClusterId)),
            0
        )
    } */
    
    override def handleLoadSnapshot(reader: SnapshotReader[T]): Unit = {
    	reader.close()
    }

    override def handleLoadBootstrap(reader: SnapshotReader[T]): Unit = {
    	reader.close()
    }
  }
  //=============================================================
  
  //=============================================================
  // A Metadata publisher to capture the Observer replication offset
  // in a metadata log of a controller
  //=============================================================
  private class ObserverReplicationOffsetPublisher() extends MetadataPublisher {

    var installLeadershipListenerAction: () => Unit = _
  
    override def name(): String = s"ObserverReplicationOffsetPublisher id=${config.nodeId}"

    override def onMetadataUpdate(delta: MetadataDelta,
        newImage: MetadataImage,
        manifest: LoaderManifest): Unit = {

      // Update the replication offset cache so that if the Observer is started,
      // it is starting from this offset
      if (newImage.observer.clusterId() == sourceClusterId) {
        if (newImage.observer.replicationOffset().isPresent()) {
          KafkaRaftObserver.this.observerReplicationOffsetCache = newImage.observer.replicationOffset().getAsLong()
        }
      }
      
      // Now that the cache had a chance to get populated from the controller local log
      // the observer can be started
      if (installLeadershipListenerAction != null)
        installLeadershipListener()
    }
    
    private def installLeadershipListener(): Unit = synchronized {

      if (installLeadershipListenerAction != null) {
        logger.info("Installing the controller leadership listener")
        logger.info("Replication offset is {}", observerReplicationOffsetCache)

        installLeadershipListenerAction()
        // The listener should be installed only once
        installLeadershipListenerAction = null
      }
    }
  }
  //=============================================================
}
