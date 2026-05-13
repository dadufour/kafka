package kafka.server.metadata

import kafka.utils.Logging
import kafka.server.MetadataReplicator
import org.apache.kafka.image.{MetadataDelta, MetadataImage}
import org.apache.kafka.image.loader.LoaderManifest
import org.apache.kafka.image.publisher.MetadataPublisher
import org.apache.kafka.raft.LeaderAndEpoch


class ControllerObserverMetadataPublisher(
        replicator: MetadataReplicator
		) extends MetadataPublisher with Logging {

  logIdent = s"[ControllerObserverMetadataPublisher]"

  override def name(): String = "ControllerObserverMetadataPublisher"

  override def onControllerChange(newLeaderAndEpoch: LeaderAndEpoch): Unit = {
    
    if (newLeaderAndEpoch.isLeader(replicator.nodeId))
  	  replicator.onBecomeLocalLeader(newLeaderAndEpoch.epoch())
  	else
  	  replicator.onBecomeLocalFollower(newLeaderAndEpoch.epoch())
  }

  override def onMetadataUpdate(delta: MetadataDelta,
		  newImage: MetadataImage,
		  manifest: LoaderManifest): Unit = {
  }
}
