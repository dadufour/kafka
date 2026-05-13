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

//import kafka.cluster.Partition
import kafka.log.LogManager
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils._
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{TopicPartition}   // Uuid  Endpoint
import org.apache.kafka.image.LocalReplicaChanges.PartitionInfo
import org.apache.kafka.image.{MetadataImage, TopicsDelta}
// import org.apache.kafka.metadata.MetadataCache
import org.apache.kafka.metadata.BrokerRegistration
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.network.BrokerEndPoint
import org.apache.kafka.storage.internals.log.LogDirFailureChannel
import org.apache.kafka.storage.log.metrics.BrokerTopicStats

import java.util
import java.util.concurrent.{CompletableFuture, ConcurrentHashMap}
import java.util.function.Predicate
import scala.collection.{mutable}
import scala.jdk.CollectionConverters._


/**
 * Trait to represent the state of replicated partitions. We create a concrete (active) Partition
 * instance when the source broker AND the destination broker receive a metadata log record 
 * indicating that they both should be leader of a partition
 */
/* 
sealed trait ReplicatedPartition

object ReplicatedPartition {

  // This broker does not have any state for this partition locally.
  final object None extends ReplicatedPartition

  // This broker hosts the partition and it is online.
  final case class Online(partition: Partition) extends ReplicatedPartition

  // This broker hosts the partition, but it is in an offline log directory.
  final case class Offline(partition: Option[Partition]) extends ReplicatedPartition
}*/


class LinkerManager(val config: KafkaConfig,
                     metrics: Metrics,
                     time: Time,
                     replicaManager: ReplicaManager,
                     val logManager: LogManager,
                     val quotaManagers: QuotaManagers,
                     logDirFailureChannel: LogDirFailureChannel,
                     val brokerTopicStats: BrokerTopicStats = new BrokerTopicStats()
                     ) extends Logging {

  private val localBrokerId = config.brokerId
  // private val allPartitions = new ConcurrentHashMap[TopicPartition, ReplicatedPartition]
  private val allSrcPartitions = new ConcurrentHashMap[TopicPartition, PartitionInfo]
  private val allTgtPartitions = new ConcurrentHashMap[TopicPartition, PartitionInfo]
  // TODO: from config
  private val sourceListenerName = ListenerName.normalised(s"INTERBROKER")

  @volatile private var currentSourceImage: MetadataImage = _
  @volatile private var currentTargetImage: MetadataImage = _
  
  @volatile private var replicaLinkerManager: ReplicaLinkerManager = _

  // A future that is complete when the instance is ready to be started
  val isReadyFuture = new CompletableFuture[Void]
  @volatile private var _isReadyOnce: Boolean = false

  this.logIdent = s"[LinkerManager broker=$localBrokerId] "


  def startup(): Unit = {

    if (!isReady()) {
      error(s"Starting LinkerManager while not ready!")
      // TODO: send exception!
      return
    }
  
    info(s"Starting up")
  
    if (replicaLinkerManager == null)
    	replicaLinkerManager = createReplicaLinkerManager(metrics, time, quotaManagers.follower)
  }

  def shutdown(): Unit = {

    info(s"Shutting down")

    if (!isReady())
      isReadyFuture.completeExceptionally(new TimeoutException())
  
    if (replicaLinkerManager != null) {
      replicaLinkerManager.shutdown()
  	  replicaLinkerManager = null
  	}
  	
    info(s"Shutdown complete")
  }

  protected def createReplicaLinkerManager(metrics: Metrics, time: Time, quotaManager: ReplicationQuotaManager) = {
    // TODO: quota!
    new ReplicaLinkerManager(config, this, replicaManager, metrics, time, quotaManager, () => targetMetadataVersion())
  }

  /**
   * Apply a KRaft topic change delta.
   *
   * @param delta           The delta to apply.
   * @param newImage        The new metadata image.
   */
  def applyTargetDelta(delta: TopicsDelta, newImage: MetadataImage): Unit = {

    info("Metadata Delta from Target")
    
    currentTargetImage = newImage

	// If first publish, the instance has now the target image
	maybeSetReady()
    
    // The instance is not started yet, we stop here
    if (replicaLinkerManager == null) return
    return

    val localChanges = delta.localChanges(localBrokerId)
    var changed = false

    // Remove partitions that have been deleted
   	changed = changed || allTgtPartitions.keySet().removeAll(localChanges.deletes)
   	replicaLinkerManager.removeFetcherForPartitions(localChanges.deletes.asScala.toSet)
   	
   	// Remove partitions that are now followers
   	changed = changed || allTgtPartitions.keySet().removeAll(localChanges.followers.keySet())
   	replicaLinkerManager.removeFetcherForPartitions(localChanges.followers.keySet().asScala.toSet)
   	
   	// Add all leader partitions
    val partitionAndOffsets = mutable.Map[TopicPartition, InitialFetchState]()
   	localChanges.leaders.asScala.foreach { case (tp, info) => {

      // Only if a new leadership
   	  if (!allTgtPartitions.contains(tp)) {

		// TODO: retrieve DST broker from source topic
        val broker = sourceBroker(10)
	    if (broker.isEmpty) {
		  warn("broker NOT found")
	    }
  	    else {
		  warn("ADDING A FETCHER on broker " + broker.get.id().toString + " with epoch=" + info.partition().leaderEpoch.toString)
		  val node = broker.get.node(sourceListenerName.value)
		  if (node.isPresent) {
			val initialFetchState = InitialFetchState(Option.empty /*Some(info.topicId())*/, 
				new BrokerEndPoint(broker.get.id(), node.get.host(), node.get.port()),
				currentLeaderEpoch = newImage.topics().getTopic(tp.topic()).partitions().get(tp.partition()).leaderEpoch,
	           	initOffset = 0)
		
			partitionAndOffsets += tp -> initialFetchState
			changed = changed || allTgtPartitions.put(tp, info)==null
//			addOnlinePartition(tp, )
		  }
	    }
   	  }
   	  
   	} }
   	
   	if (!partitionAndOffsets.isEmpty)
      replicaLinkerManager.addFetcherForPartitions(partitionAndOffsets)

    replicaLinkerManager.shutdownIdleFetcherThreads()



//    replicaStateChangeLock.synchronized {
      // Handle deleted partitions. We need to do this first because we might subsequently
      // create new partitions with the same names as the ones we are deleting here.

//      if (sourceMetadataVersion().isDirectoryAssignmentSupported) {
        // We only want to update the directoryIds if DirectoryAssignment is supported!
//        localChanges.directoryIds.forEach(maybeUpdateTopicAssignment)
//      }
//    }
  }


  def applySourceDelta(delta: TopicsDelta, newImage: MetadataImage): Unit = {
  
    info("Metadata Delta from Source")
    
    currentSourceImage = newImage

	// If first publish, the instance has now the source image
	maybeSetReady()

    // The instance is not started yet, we stop here
    if (replicaLinkerManager == null) return


	// Iterate on the source brokers
    val partitionAndOffsets = mutable.Map[TopicPartition, InitialFetchState]()
	newImage.cluster().brokers().values.forEach { broker => {
	
	  val localChanges = delta.localChanges(broker.id())
      var changed = false

      // Remove partitions that have been deleted
   	  changed = changed || allSrcPartitions.keySet().removeAll(localChanges.deletes)
   	  replicaLinkerManager.removeFetcherForPartitions(localChanges.deletes.asScala.toSet)
   	
   	  // Remove partitions that are now followers
   	  changed = changed || allSrcPartitions.keySet().removeAll(localChanges.followers.keySet())
   	  replicaLinkerManager.removeFetcherForPartitions(localChanges.followers.keySet().asScala.toSet)

   	  // Add all leader partitions
      localChanges.leaders.asScala.foreach { case (tp, info) => {
   	    
        // Only if a new leadership
        if (!allSrcPartitions.contains(tp)) {

		  warn("ADDING A FETCHER on broker " + broker.id().toString + " with epoch=" + newImage.topics().getTopic(tp.topic()).partitions().get(tp.partition()).leaderEpoch.toString + " with TopicId=" + info.topicId())
		  val node = broker.node(sourceListenerName.value)
		  if (node.isPresent) {
			val initialFetchState = InitialFetchState(Some(info.topicId()) /*Option.empty*/, 
				new BrokerEndPoint(broker.id(), node.get.host(), node.get.port()),
				currentLeaderEpoch = newImage.topics().getTopic(tp.topic()).partitions().get(tp.partition()).leaderEpoch,
	           	initOffset = 0)
		
			partitionAndOffsets += tp -> initialFetchState
			changed = changed || allSrcPartitions.put(tp, info)==null
//			addOnlinePartition(tp, )
		  }
   	    }
   	    
      } }	
	
	} }
	
	if (!partitionAndOffsets.isEmpty)
      replicaLinkerManager.addFetcherForPartitions(partitionAndOffsets)

    replicaLinkerManager.shutdownIdleFetcherThreads()
  }

  private def sourceBroker(brokerId: Int): util.Optional[BrokerRegistration] = {
  
    if (currentSourceImage != null)
  	  util.Optional.ofNullable(currentSourceImage.cluster().broker(brokerId))
        .filter(Predicate.not(_.fenced))
    else
      util.Optional.empty
  }

  private def maybeSetReady(): Unit = {
  
    if (!_isReadyOnce && currentTargetImage!=null && currentSourceImage!=null) {
        info("Linker now ready to start")
    	isReadyFuture.complete(null)
    	_isReadyOnce = true
    }
  }

  private def isReady(): Boolean = {
  
    _isReadyOnce
  }

/*  
  private def sourceMetadataVersion(): MetadataVersion = {
  
  	currentSourceImage.features().metadataVersionOrThrow()
  } */

  private def targetMetadataVersion(): MetadataVersion = {
  
  	currentTargetImage.features().metadataVersionOrThrow()
  }

//  private def addOnlinePartition(topicPartition: TopicPartition, partition: Partition): Unit = {
//    allPartitions.put(topicPartition, ReplicatedPartition.Online(partition))
//  }
   
}
