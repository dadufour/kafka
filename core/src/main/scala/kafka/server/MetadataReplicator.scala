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

import kafka.utils.Logging


/**********************************************************
* MetadataReplicator
*
* Receive Metadata events from a different cluster (controller)
* and replicate the events in the local cluster (controller)
* Used by Controllers, only active when the controller is leader
***********************************************************/
class MetadataReplicator(
        val config: KafkaConfig
                     ) extends Logging {

  def nodeId = config.nodeId
  private var isActive = false
  private var lastEpoch: Int = 0

  this.logIdent = s"[MetadataReplicator controller=$nodeId] "

  // When the local controller becomes leader
  def onBecomeLocalLeader(epoch: Int): Unit = {

	if (!isActive)
	isActive = true
  	if (lastEpoch != epoch) {
  	  lastEpoch = epoch
  	}
  }

  // When the local controller becomes follower
  def onBecomeLocalFollower(epoch: Int): Unit = {

	isActive = false 
  	if (lastEpoch != epoch) {
  	  lastEpoch = epoch
  	}
  }
   
}
