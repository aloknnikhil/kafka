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

import org.apache.kafka.common.metadata.TopicRecord
import org.apache.kafka.common.protocol.ApiMessage

class TopicRecordProcessor extends ApiMessageProcessor {
  override def process(brokerMetadataBasis: BrokerMetadataBasis, apiMessage: ApiMessage): BrokerMetadataBasis = {
    apiMessage match {
      case topicRecord: TopicRecord =>
        val brokerMetadataValue = brokerMetadataBasis.getValue()

        val newMetadataCacheBasis: MetadataCacheBasis = process(topicRecord, brokerMetadataValue.metadataCacheBasis)

        brokerMetadataBasis.newBasis(brokerMetadataValue.newValue(newMetadataCacheBasis))
      case unexpected => throw new IllegalArgumentException(s"apiMessage was not of type TopicRecord: ${unexpected.getClass}")
    }
  }

  // visible for testing
  private[server] def process(topicRecord: TopicRecord, metadataCacheBasis: MetadataCacheBasis) = {
    throw new UnsupportedOperationException("Not yet implemented: TopicRecord")
  }
}
