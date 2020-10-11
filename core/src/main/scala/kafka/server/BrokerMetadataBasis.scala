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

/**
 * This class provides the ability to retrieve the current state of broker metadata and evolve that state to
 * a new state across several changes before writing the state back to the broker and exposing it externally.
 * This allows the state to change over multiple operations without exposing intermediate states.
 * This class is not thread-safe.
 */
class BrokerMetadataBasis(metadataCache: MetadataCache) {

  private var metadataSnapshot: Option[MetadataSnapshot] = None
  def getMetadataSnapshot(): MetadataSnapshot = {
    if (metadataSnapshot.isEmpty) {
      metadataSnapshot = Some(metadataCache.getMetadataSnapshot())
    }
    metadataSnapshot.get
  }
  def setMetadataSnapshot(metadataSnapshot: MetadataSnapshot): Unit = {
    this.metadataSnapshot = Some(metadataSnapshot)
  }

  def writeState(): Unit = {
    metadataSnapshot.foreach(m => metadataCache.setMetadataSnapshot(m))
  }
}
