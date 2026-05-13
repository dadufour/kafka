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

import java.io.File
import java.net.InetSocketAddress
import java.nio.file.Files
import java.util
import java.util.{Optional, OptionalInt}
import java.util.{Collection => JCollection }
import kafka.server.KafkaConfig
import kafka.utils.CoreUtils
import kafka.utils.Logging
import org.apache.kafka.clients.{ApiVersions, ManualMetadataUpdater, MetadataRecoveryStrategy, NetworkClient}
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.internals.Topic
//import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ChannelBuilders, ListenerName, NetworkReceive, Selectable, Selector}
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{LogContext, Time, Utils}
import org.apache.kafka.metadata.properties.{MetaPropertiesEnsemble, MetaProperties}
import org.apache.kafka.metadata.properties.MetaPropertiesEnsemble.VerificationFlag._
import org.apache.kafka.raft.{Endpoints, FileQuorumStateStore, KafkaNetworkChannel, KafkaRaftClient, KafkaRaftClientDriver, MetadataLogConfig, QuorumConfig, RaftClient, ReplicatedLog, TimingWheelExpirationService}
import org.apache.kafka.raft.ExternalKRaftMetrics
import org.apache.kafka.server.fault.LoggingFaultHandler
import org.apache.kafka.server.common.Feature
import org.apache.kafka.server.common.serialization.RecordSerde
import org.apache.kafka.server.util.{FileLock, KafkaScheduler}
import org.apache.kafka.server.util.timer.SystemTimer
import org.apache.kafka.storage.internals.log.{LogManager, UnifiedLog}

import scala.jdk.CollectionConverters._

object KafkaRaftObserver {

  def generateObserverBrokerId(localClusterId: String, brokerId: Int): Int = {
      // TODO
      5000 + brokerId
  }
  
  private def getLogDirectory(linkingLogDir: String, sourceClusterId: String): String = {

	// Each source cluster has its own log directory
    val dir = new File(linkingLogDir, sourceClusterId)
    Files.createDirectories(dir.toPath)

    dir.getAbsolutePath
  }
  
  private def initializeLogDir(metadatalogDir: String, sourceClusterId: String, brokerId: Int): MetaProperties = {

    // Load and verify the original ensemble.
    val loader = new MetaPropertiesEnsemble.Loader()
    loader.addMetadataLogDir(metadatalogDir)
    val initialMetaPropsEnsemble = loader.load()
    
    // Check if the log directory already exists
    val metaProps: Option[MetaProperties] = Option(initialMetaPropsEnsemble.logDirProps().get(metadatalogDir))
    var meta: MetaProperties = null
    if (!metaProps.nonEmpty) {

  	  // Log directory does not exist => we generate one
  	  val copier = new MetaPropertiesEnsemble.Copier(initialMetaPropsEnsemble)
      val builder = new MetaProperties.Builder
      meta = builder.setClusterId(sourceClusterId)
    	    .setNodeId(brokerId)
    	    .setDirectoryId(copier.generateValidDirectoryId())		// DOES NOT ENSURE UNICITY IN THIS CLUSTER
    	    .build()

//      copier.setRandom(/*TODO*/)
      copier.setLogDirProps(metadatalogDir, meta)
      copier.setMetaLogDir(Optional.of(metadatalogDir))
//      copier.setPreWriteHandler((metadatalogDir, _, _) => {
//          log.info("{}Rewriting {}{}meta.properties", logPrefix, logDir, File.separator)
//        })
        
      copier.writeLogDirChanges()
    }
    else {
    
    	// Log directory already exists => we validate
        val verificationFlags = util.EnumSet.of(REQUIRE_AT_LEAST_ONE_VALID)
        initialMetaPropsEnsemble.verify(Optional.of(sourceClusterId), OptionalInt.of(brokerId), verificationFlags)
        
        meta = metaProps.get
    }
    
    meta
  }
}

class KafkaRaftObserver[T](
  clusterId: String,
  config: KafkaConfig,
  serde: RecordSerde[T],
  time: Time,
  metrics: Metrics,
  bootstrapServers: JCollection[InetSocketAddress]
) extends Logging {

  private val threadNamePrefix = "linking-raft"
  private val logContext = new LogContext(s"[RaftObserver Cluster id=${clusterId}] ")
  this.logIdent = logContext.logPrefix()

  private val scheduler = new KafkaScheduler(1, true, threadNamePrefix + "-scheduler")
  scheduler.startup()

  // Define Log directory for the observed cluster
  private val observerLogDirectory = KafkaRaftObserver.getLogDirectory(config.metadataLogDir, clusterId)
  // Define a broker Id for this observer with the observed cluster
  
  private val logDirProps = KafkaRaftObserver.initializeLogDir(
  	observerLogDirectory, 
  	clusterId, 
  	config.nodeId)

  val metadataLogDirUuid: Uuid = logDirProps.directoryId().get

  private val logDirLock = lockLogDir()
  private val dataDir = createDataDir()

  private val replicatedLog: ReplicatedLog = buildMetadataLog()
  private val netChannel = buildNetworkChannel()
  private val expirationTimer = new SystemTimer("linking-expiration-executor")
  private val expirationService = new TimingWheelExpirationService(expirationTimer)
  private val client: KafkaRaftClient[T] = buildRaftClient()
  private val clientDriver = new KafkaRaftClientDriver[T](client, threadNamePrefix, 
  	new LoggingFaultHandler(s"raft observer", () => {}), 
  	logContext)

  def startup(): Unit = {
    client.initialize(
      new java.util.HashMap[Integer, InetSocketAddress](),
      new FileQuorumStateStore(new File(dataDir, FileQuorumStateStore.DEFAULT_FILE_NAME)),
      metrics,
	  new ExternalKRaftMetrics {
        override def setIgnoredStaticVoters(ignoredStaticVoters: Boolean): Unit = { }
	  },
	  s"observer"
    )
    netChannel.start()
    clientDriver.start()
  }

  def shutdown(): Unit = {
    CoreUtils.swallow(expirationService.shutdown(), this)
    Utils.closeQuietly(expirationTimer, "expiration timer")
    CoreUtils.swallow(clientDriver.shutdown(), this)
    CoreUtils.swallow(scheduler.shutdown(), this)
    Utils.closeQuietly(netChannel, "net channel")
    Utils.closeQuietly(replicatedLog, "replicated log")
    CoreUtils.swallow(logDirLock.destroy(), this)
  }

  def register(
    listener: RaftClient.Listener[T]
  ): Unit = {
    client.register(listener)
  }

  private def buildRaftClient(): KafkaRaftClient[T] = {
    new KafkaRaftClient(
      OptionalInt.empty(),		// Node Id: must be null for non-controller
      metadataLogDirUuid,		// Uuid of observer local log directory
      serde,
      netChannel,
      replicatedLog,
      time,
      expirationService,
      logContext,
      false,
      clusterId,				// Cluster Id of remote observed cluster
      bootstrapServers,			// Bootstrap of remote observed cluster
      Endpoints.empty(),
      Feature.KRAFT_VERSION.supportedVersionRange(),
      new QuorumConfig(config)
    )
  }

  private def buildNetworkChannel(): KafkaNetworkChannel = {
    val (listenerName, netClient) = buildNetworkClient()
    new KafkaNetworkChannel(time, listenerName, netClient, config.quorumConfig.requestTimeoutMs, threadNamePrefix)
  }

  private def createDataDir(): File = {
    val dir = new File(observerLogDirectory, UnifiedLog.logDirName(Topic.CLUSTER_METADATA_TOPIC_PARTITION))
    Files.createDirectories(dir.toPath)
    dir
  }

  private def lockLogDir(): FileLock = {
    val lock = new FileLock(new File(observerLogDirectory, LogManager.LOCK_FILE_NAME))

    if (!lock.tryLock()) {
      throw new KafkaException(
        s"Failed to acquire lock on file .lock in ${lock.file.getParent}. A Kafka instance in another process or " +
        "thread is using this directory."
      )
    }

    lock
  }

  private def buildMetadataLog(): KafkaMetadataLog = {
    KafkaMetadataLog(
      Topic.CLUSTER_METADATA_TOPIC_PARTITION,
      Uuid.METADATA_TOPIC_ID,
      dataDir,
      time,
      scheduler,
      config = new MetadataLogConfig(config),
      config.nodeId
    )
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
  
  def getClient(): RaftClient[T] = {
    client
  }  
}
