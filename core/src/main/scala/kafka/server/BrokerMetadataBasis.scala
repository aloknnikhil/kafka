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

import kafka.coordinator.group.{GroupCoordinator, GroupCoordinatorPartitionsDeleted}
import org.apache.kafka.common.protocol.ApiMessage

/**
 * There is no single place where all broker metadata lives.  For example, MetadataCache holds some of it,
 * ReplicaManager holds some of it, etc.  This class is a composite of the pieces from the various locations, and it
 * stitches the pieces together into a single value.  Each individual piece may be a {@link Basis} when
 * it is the only write path to that part of the metadata, and such pieces can change independently of each other.
 * The parts of the metadata that are not the only write path are represented by actions that can be applied.
 *
 * @param metadataCacheBasis the MetadataCacheBasis defining part of the value
 * @param groupCoordinatorPartitionsDeleted the action indicating to the group coordinator what partitions (if any)
 *                                          have been deleted
 * @param updateClientQuotaCallbackMetricConfigs the action giving the client quota callback (if any) an opportunity to
 *                                               decide if quota metric configs need to be updated based on current
 *                                               cluster metadata
 */
case class BrokerMetadataValue(metadataCacheBasis: MetadataCacheBasis,
                               groupCoordinatorPartitionsDeleted: GroupCoordinatorPartitionsDeleted,
                               updateClientQuotaCallbackMetricConfigs: UpdateClientQuotaCallbackMetricConfigs
                              ) {
  def newValue(metadataCacheBasis: MetadataCacheBasis): BrokerMetadataValue = {
    // keep all parts except the one given
    BrokerMetadataValue(metadataCacheBasis, groupCoordinatorPartitionsDeleted, updateClientQuotaCallbackMetricConfigs)
  }

  def newValue(groupCoordinatorPartitionsDeleted: GroupCoordinatorPartitionsDeleted): BrokerMetadataValue = {
    // keep all parts except the one given
    BrokerMetadataValue(metadataCacheBasis, groupCoordinatorPartitionsDeleted, updateClientQuotaCallbackMetricConfigs)
  }

  def newValue(updateClientQuotaCallbackMetricConfigs: UpdateClientQuotaCallbackMetricConfigs): BrokerMetadataValue = {
    // keep all parts except the one given
    BrokerMetadataValue(metadataCacheBasis, groupCoordinatorPartitionsDeleted, updateClientQuotaCallbackMetricConfigs)
  }

  /**
   * For each underlying basis, write it if necessary
   */
  def writeIfNecessary() : Unit = {
    metadataCacheBasis.writeIfNecessary()
    groupCoordinatorPartitionsDeleted.groupCoordinatorHandlePartitionsDeletedIfNecessary()
  }
}

/**
 * There is no single place where all broker metadata lives.  For example, MetadataCache holds some of it,
 * ReplicaManager holds some of it, etc.  This class can retrieve all of the pieces from the various locations
 * and stitch them together into a single BrokerMetadataValue, and it can take such a value and push its pieces out
 * to all the necessary destinations.  Each individual piece may be a {@link Basis} when
 * it is the only write path to that part of the metadata, and such pieces can change independently of each other.
 * The parts of the metadata that are not the only write path are represented by actions that can be applied.
 *
 * @param kafkaConfig the Kafka configuration
 * @param clusterId the Kafka cluster Id
 * @param metadataCache the MetadataCache where part of the value is retrieved/written
 * @param groupCoordinator the group coordinator
 * @param quotaManagers the quota managers
 */
private class BrokerMetadataHolder(kafkaConfig: KafkaConfig,
                                   clusterId: String,
                                   metadataCache: MetadataCache,
                                   groupCoordinator: GroupCoordinator,
                                   quotaManagers: QuotaFactory.QuotaManagers) extends ValueHolder[BrokerMetadataValue] {
  override def retrieveValue(): BrokerMetadataValue = {
    BrokerMetadataValue(new MetadataCacheBasis(metadataCache),
      new GroupCoordinatorPartitionsDeleted(groupCoordinator),
    new UpdateClientQuotaCallbackMetricConfigs(kafkaConfig, quotaManagers, metadataCache, clusterId))
  }

  override def writeValue(value: BrokerMetadataValue): Unit = {
    value.writeIfNecessary()
  }
}

/**
 * There is no single place where all broker metadata lives.  For example, MetadataCache holds some of it,
 * ReplicaManager holds some of it, etc.  This class stitches the various pieces together and presents them as a single
 * {@link Basis}.  Each individual piece may itself be a {@link Basis} when
 * it is the only write path to that part of the metadata, and such pieces can change independently of each other.
 * The parts of the metadata that are not the only write path are represented by actions that can be applied.
 * There are methods to create a new basis from the current one with changes to an underlying basis or an action.
 *
 * @param kafkaConfig the Kafka configuration
 * @param clusterId the Kafka cluster Id
 * @param metadataCache the MetadataCache that defines one underlying basis from which this one is constructed.
 * @param groupCoordinator the group coordinator
 * @param quotaManagers the quota managers
 */
class BrokerMetadataBasis(val kafkaConfig: KafkaConfig,
                          clusterId: String,
                          metadataCache: MetadataCache,
                          val groupCoordinator: GroupCoordinator,
                          val quotaManagers: QuotaFactory.QuotaManagers)
  extends Basis[BrokerMetadataValue](
    new BrokerMetadataHolder(kafkaConfig, clusterId, metadataCache, groupCoordinator, quotaManagers)) {

  override def newBasis(valueToWrite: BrokerMetadataValue): BrokerMetadataBasis = {
    super.newBasis(valueToWrite).asInstanceOf[BrokerMetadataBasis]
  }
}

/**
 * Ability to process an ApiMessage based on an existing broker metadata basis
 */
trait ApiMessageProcessor {
  def process(brokerMetadataBasis: BrokerMetadataBasis, apiMessage: ApiMessage): BrokerMetadataBasis
}

