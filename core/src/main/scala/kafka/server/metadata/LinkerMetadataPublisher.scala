package kafka.server.metadata

import kafka.utils.Logging
import kafka.server.LinkerManager
import org.apache.kafka.image.{MetadataDelta, MetadataImage}
import org.apache.kafka.image.loader.LoaderManifest
import org.apache.kafka.image.publisher.MetadataPublisher


class LinkerMetadataPublisher(
		linkerMgr: LinkerManager,
		val asSource: Boolean
		) extends MetadataPublisher with Logging {

  logIdent = if (asSource) s"[LinkerMetadataPublisher(source)]" else s"[LinkerMetadataPublisher(target)]"

  override def name(): String = "LinkerMetadataPublisher"

  override def onMetadataUpdate(delta: MetadataDelta,
		  newImage: MetadataImage,
		  manifest: LoaderManifest): Unit = {

		val topicsDelta = delta.topicsDelta()
		if (topicsDelta != null) {
			if (asSource)
    			linkerMgr.applySourceDelta(delta.topicsDelta(), newImage)
    		else
    			linkerMgr.applyTargetDelta(delta.topicsDelta(), newImage)
    	}
    }
}
