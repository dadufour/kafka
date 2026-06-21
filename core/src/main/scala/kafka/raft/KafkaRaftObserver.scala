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

import java.net.InetSocketAddress
import java.util.{OptionalInt, Collection => JCollection, Map => JMap}
import kafka.server.KafkaConfig
import kafka.utils.Logging
import org.apache.kafka.clients.{ApiVersions, ManualMetadataUpdater, MetadataRecoveryStrategy, NetworkClient}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ChannelBuilders, ListenerName, NetworkReceive, Selectable, Selector}
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.utils.internals.LogContext
import org.apache.kafka.raft.internals.EphemeralRaftLog
import org.apache.kafka.raft.{Endpoints, ExternalKRaftMetrics, VoidQuorumStateStore, KafkaNetworkChannel, KafkaRaftClient, KafkaRaftClientDriver, QuorumConfig, RaftLog, TimingWheelExpirationService, RaftClient, RaftManager, BatchReader, LeaderAndEpoch}
import org.apache.kafka.server.ProcessRole
import org.apache.kafka.server.common.Feature
import org.apache.kafka.server.common.serialization.RecordSerde
import org.apache.kafka.server.fault.FaultHandler
import org.apache.kafka.server.util.timer.SystemTimer
import org.apache.kafka.snapshot.SnapshotReader

import scala.jdk.CollectionConverters._


class KafkaRaftObserver[T](
  clusterId: String,
  config: KafkaConfig,
  metadataLogDirUuid: Uuid,
  serde: RecordSerde[T],
  topicPartition: TopicPartition,
  topicId: Uuid,
  time: Time,
  metrics: Metrics,
  externalKRaftMetrics: ExternalKRaftMetrics,
  threadNamePrefixOpt: Option[String],
  bootstrapServers: JCollection[InetSocketAddress],
  fatalFaultHandler: FaultHandler
) extends Logging {

  val apiVersions = new ApiVersions()
  private val raftConfig = new QuorumConfig(config)
  private val threadNamePrefix = threadNamePrefixOpt.getOrElse("kafka-raft")
  private val logContext = new LogContext(s"[RaftObserver id=${config.nodeId}] ")
  this.logIdent = logContext.logPrefix()


  private class RunningObserver() {
  
    private val netChannel = buildNetworkChannel()
    private val client: KafkaRaftClient[T] = buildRaftClient()
    private val clientDriver = new KafkaRaftClientDriver[T](client, threadNamePrefix, fatalFaultHandler, logContext)
    
    private def buildRaftClient(): KafkaRaftClient[T] = {
      val client = new KafkaRaftClient(
        OptionalInt.of(config.nodeId),
        metadataLogDirUuid,
        serde,
        netChannel,
        raftLog,
        time,
        expirationService,
        logContext,
        // Controllers should always flush the log on replication because they may become voters
        config.processRoles.contains(ProcessRole.ControllerRole),
        clusterId,
        bootstrapServers,
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
        apiVersions,
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
  	      externalKRaftMetrics,
	      s"observer"
      )
      netChannel.start()
      clientDriver.start()
    }
    
    def stop(): Unit = {
      clientDriver.shutdown()
      netChannel.close()
    }
    
  }

  private var runningObserver: RunningObserver = _
  private val raftLog: RaftLog = buildMetadataLog()
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
  	stop()
    Utils.swallow(this.logger.underlying, () => expirationService.shutdown())
    Utils.closeQuietly(expirationTimer, "expiration timer")
    Utils.closeQuietly(raftLog, "raft log")
  }

  private def buildMetadataLog(): RaftLog = {
    EphemeralRaftLog.createLog(
      topicPartition,
      topicId,
      config.nodeId
    )
  }


  // Starter of this instance by listening on a Controller Raft manager
  def buildStarter[U](controllerRaftManager: RaftManager[U],
    				  controllerId: Int
    				  ): Unit = {
    	controllerRaftManager.client.register(new RaftLeadershipListener[U](controllerId))
  }
  
  private class RaftLeadershipListener[U](
    val controllerId: Int
  ) extends RaftClient.Listener[U] {

    override def handleLeaderChange(newLeaderAndEpoch: LeaderAndEpoch): Unit = {
      if (newLeaderAndEpoch.isLeader(controllerId)) {
        if (KafkaRaftObserver.this.isRunning() == false) {
            KafkaRaftObserver.this.logger.info("Starting RaftObserver because controller {} became leader", controllerId)
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
  
  
  private class RaftObserverListener(
  ) extends RaftClient.Listener[T] {

    override def handleLeaderChange(newLeaderAndEpoch: LeaderAndEpoch): Unit = {
    }

    override def handleCommit(reader: BatchReader[T]): Unit = {
    
      	var lastOffset: Long = -1;
      	try {
        	while (reader.hasNext()) {
            	val batch = reader.next();
            
            	lastOffset = batch.lastOffset();
        	}
      	} finally {
        	reader.close();
      	}
      	if (lastOffset >= 0) {
        	KafkaRaftObserver.this.logger.info("new replication offset {}", lastOffset)
      	}
    }

    override def handleLoadSnapshot(reader: SnapshotReader[T]): Unit = {
    	reader.close()
    }

    override def handleLoadBootstrap(reader: SnapshotReader[T]): Unit = {
    	reader.close()
    }
  }
  
}
