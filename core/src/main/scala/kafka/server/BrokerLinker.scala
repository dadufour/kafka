/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import kafka.log.LogManager
import kafka.raft.KafkaRaftObserver
import kafka.server.metadata.LinkerMetadataPublisher
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.{LogContext, Time, Utils}
import org.apache.kafka.image.loader.MetadataLoader
import org.apache.kafka.image.publisher.{MetadataPublisher, SnapshotEmitter, SnapshotGenerator}
import org.apache.kafka.metadata.MetadataRecordSerde
import org.apache.kafka.raft.MetadataLogConfig
import org.apache.kafka.server.common.ApiMessageAndVersion
import org.apache.kafka.server.config.{KRaftConfigs, ServerConfigs}
import org.apache.kafka.server.util.{Deadline, FutureUtils}
import org.apache.kafka.storage.internals.log.LogDirFailureChannel
import org.apache.kafka.storage.log.metrics.BrokerTopicStats

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import java.util.Arrays
import scala.jdk.CollectionConverters._


class BrokerLinker(
  config: KafkaConfig,
  time: Time,
  startupDeadline: Deadline,
  metrics: Metrics,
  replicaManager: ReplicaManager,
  val logManager: LogManager,
  val quotaManagers: QuotaFactory.QuotaManagers,
  logDirFailureChannel: LogDirFailureChannel,
  val brokerTopicStats: BrokerTopicStats = new BrokerTopicStats()
) extends Logging {
  
  this.logIdent = s"[BrokerLinker id=${config.nodeId}] "
  
  val observerNodeId = KafkaRaftObserver.generateObserverBrokerId(s"19ZzdXggTGq7U-Ox3rsljQ", config.nodeId)
  val logContext: LogContext = new LogContext(s"[BrokerLinker id=${config.nodeId}] ")
  @volatile private var raftObserver: KafkaRaftObserver[ApiMessageAndVersion] = _
  @volatile private var loader: MetadataLoader = _
  @volatile private var snapshotGenerator: SnapshotGenerator = _
  private val linkerMgr = new LinkerManager(config,
                     metrics,
                     time,
                     replicaManager,
                     logManager,
                     quotaManagers,
                     logDirFailureChannel,
                     brokerTopicStats)
  val targetMetadataPublisher: Option[MetadataPublisher] = if (isVoid()) Option.empty else Some(new LinkerMetadataPublisher(linkerMgr, false))

  private var _started = false
  
  private def isVoid(): Boolean = {
    // TODO: if not required (from config)
    false
  } 

  def maybeStartup(): Unit = {
    
    if (isVoid()) return
    
    _started = true
    
    // Change the logdir and nodeId for observer 
    val origProps = config.originals()
    origProps.put(MetadataLogConfig.METADATA_LOG_DIR_CONFIG, s"/tmp/kafka/clusterB/node10/linking")
    origProps.put(ServerConfigs.BROKER_ID_CONFIG, observerNodeId.toString)
    origProps.put(KRaftConfigs.NODE_ID_CONFIG, observerNodeId.toString)
    val observerConfig = KafkaConfig.apply(origProps, false)
    raftObserver = new KafkaRaftObserver[ApiMessageAndVersion](
	  	s"19ZzdXggTGq7U-Ox3rsljQ",	// Source cluster Id
  		observerConfig,
  		new MetadataRecordSerde,
  		time,
  		metrics,
  		List(new InetSocketAddress("127.0.0.1", 9093)).asJava)
    raftObserver.startup()
    
    val loaderBuilder = new MetadataLoader.Builder().
      setNodeId(observerNodeId).
      setTime(time).
      setThreadNamePrefix(s"kafka-${observerNodeId}-").
//      setFaultHandler(metadataLoaderFaultHandler).
      setHighWaterMarkAccessor(() => raftObserver.getClient().highWatermark()) //.
//      setMetrics(metadataLoaderMetrics)
    loader = loaderBuilder.build()
    
    val snapshotEmitter = new SnapshotEmitter.Builder().
      setNodeId(observerNodeId).
      setRaftClient(raftObserver.getClient()).
//      setMetrics(new SnapshotEmitterMetrics(
//        Optional.of(KafkaYammerMetrics.defaultRegistry()), time)).
      build()
      
    snapshotGenerator = new SnapshotGenerator.Builder(snapshotEmitter).
      setNodeId(observerNodeId).
      setTime(time).
//      setFaultHandler(metadataPublishingFaultHandler).
      setMaxBytesSinceLastSnapshot(config.metadataSnapshotMaxNewRecordBytes).
      setMaxTimeSinceLastSnapshotNs(TimeUnit.MILLISECONDS.toNanos(config.metadataSnapshotMaxIntervalMs)).
//      setDisabledReason(snapshotsDisabledReason).
      setThreadNamePrefix(s"linker-${observerNodeId}-").
      build()

    val sourcePublisher = new LinkerMetadataPublisher(linkerMgr, true)      
    try {
      loader.installPublishers(Arrays.asList(snapshotGenerator, sourcePublisher)).get()
    } catch {
      case t: Throwable => {
        error("Unable to install metadata publishers", t)
        throw new RuntimeException("Unable to install metadata publishers.", t)
      }
    }
    raftObserver.register(loader)
    
    // Block here until the broker linker is ready for startup
    FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "broker linker future to be completed",
        linkerMgr.isReadyFuture, startupDeadline, time)
    
    linkerMgr.startup()
  }


  def shutdown(): Unit = {

    if (!_started) return
    
    CoreUtils.swallow(linkerMgr.shutdown(), this)
  
    if (loader != null) {
      CoreUtils.swallow(loader.beginShutdown(), this)
    }
    if (snapshotGenerator != null) {
      CoreUtils.swallow(snapshotGenerator.beginShutdown(), this)
    }
    Utils.closeQuietly(loader, "loader")
    loader = null
//    Utils.closeQuietly(metadataLoaderMetrics, "metadata loader metrics")
//    metadataLoaderMetrics = null
    Utils.closeQuietly(snapshotGenerator, "snapshot generator")
    snapshotGenerator = null
    if (raftObserver != null) {
      CoreUtils.swallow(raftObserver.shutdown(), this)
      raftObserver = null
    }
//    Utils.closeQuietly(controllerServerMetrics, "controller server metrics")
//    controllerServerMetrics = null
//    Utils.closeQuietly(brokerMetrics, "broker metrics")
//    brokerMetrics = null
//    Utils.closeQuietly(nodeMetrics, "node metrics")
//    nodeMetrics = null
//    Utils.closeQuietly(metrics, "metrics")
//    metrics = null
//    CoreUtils.swallow(AppInfoParser.unregisterAppInfo(MetricsPrefix, sharedServerConfig.nodeId.toString, metrics), this)
  }
}
