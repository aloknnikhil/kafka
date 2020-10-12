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

import org.apache.kafka.common.protocol.ApiMessage

/**
 * There is no single place where all broker metadata lives.  For example, MetadataCache holds some of it,
 * ReplicaManager holds some of it, etc.  This class is a composite of the pieces from the various locations and
 * stitches them together into a single value.  Each individual piece is itself a {@link WriteableBasis} and
 * can change independently of any other piece.
 *
 * @param metadataCacheBasis the MetadataCacheBasis defining part of the value
 */
case class BrokerMetadataValue(metadataCacheBasis: MetadataCacheBasis) {
  def newValue(metadataCacheBasis: MetadataCacheBasis): BrokerMetadataValue = {
    // keep all parts except the one given
    BrokerMetadataValue(metadataCacheBasis)
  }

  /**
   * For each underlying basis, write it if necessary
   */
  def writeIfNecessary() : Unit = {
    metadataCacheBasis.writeIfNecessary()
  }
}

/**
 * There is no single place where all broker metadata lives.  For example, MetadataCache holds some of it,
 * ReplicaManager holds some of it, etc.  This class can retrieve all of the pieces from the various locations
 * and stitch them together into a single BrokerMetadataValue, and it can take such a value and push its pieces out
 * to all the necessary destinations.  Each individual piece is itself a {@link WriteableBasis} and
 * can change independently of any other piece.
 *
 * @param metadataCache the MetadataCache from which part of the value is retrieved/written
 */
private class BrokerMetadataHolder(metadataCache: MetadataCache) extends ValueHolder[BrokerMetadataValue] {
  override def retrieveValue(): BrokerMetadataValue = {
    BrokerMetadataValue(new MetadataCacheBasis(metadataCache))
  }

  override def writeValue(value: BrokerMetadataValue): Unit = {
    value.writeIfNecessary()
  }
}

/**
 * There is no single place where all broker metadata lives.  For example, MetadataCache holds some of it,
 * ReplicaManager holds some of it, etc.  This class stitches the various pieces together and presents them as a single
 * {@link WriteableBasis}.  It is itself a composite of individual {@link WriteableBasis} instances, each of which
 * can change independently of any other piece, so it provides methods to create a new basis from the current one
 * with changes to an underlying basis.
 *
 * @param metadataCache the MetadataCache that defines one underlying basis from which this one is constructed.
 */
class BrokerMetadataBasis(metadataCache: MetadataCache)
  extends WriteableBasis[BrokerMetadataValue](new BrokerMetadataHolder(metadataCache)) {

  override def newBasis(valueToWrite: BrokerMetadataValue): BrokerMetadataBasis = {
    super.newBasis(valueToWrite).asInstanceOf[BrokerMetadataBasis]
  }
}

/**
 * Ability to apply an ApiMessage on top of an existing broker metadata basis
 */
trait ApiMessageProcessor {
  def process(brokerMetadataBasis: BrokerMetadataBasis, apiMessage: ApiMessage): BrokerMetadataBasis
}

