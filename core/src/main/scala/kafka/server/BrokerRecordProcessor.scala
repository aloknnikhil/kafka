package kafka.server
import java.util

import kafka.cluster.{Broker, EndPoint}
import org.apache.kafka.common.Node
import org.apache.kafka.common.metadata.BrokerRecord
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.ApiMessage
import org.apache.kafka.common.security.auth.SecurityProtocol

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

class BrokerRecordProcessor extends ApiMessageProcessor {
  override def applyTo(brokerMetadataBasis: BrokerMetadataBasis, apiMessage: ApiMessage): BrokerMetadataBasis = {
      apiMessage match {
        case brokerRecord: BrokerRecord =>
          val brokerMetadataValue = brokerMetadataBasis.getValue()

          val newMetadataCacheBasis: MetadataCacheBasis = applyTo(brokerRecord, brokerMetadataValue.metadataCacheBasis)

          brokerMetadataBasis.newBasis(brokerMetadataValue.newValue(newMetadataCacheBasis))
        case unexpected => throw new IllegalArgumentException(s"apiMessage was not of type BrokerRecord: ${unexpected.getClass}")
      }
  }

  // visible for testing
  private[server] def applyTo(brokerRecord: BrokerRecord, metadataCacheBasis: MetadataCacheBasis) = {
    val metadataSnapshot = metadataCacheBasis.getValue()
    val upsertBrokerId = brokerRecord.brokerId()
    val existingUpsertBrokerState = metadataSnapshot.aliveBrokers.get(upsertBrokerId)
    // allocate new alive brokers/nodes
    val newAliveBrokers = new mutable.LongMap[Broker](metadataSnapshot.aliveBrokers.size + (if (existingUpsertBrokerState.isEmpty) 1 else 0))
    val newAliveNodes = new mutable.LongMap[collection.Map[ListenerName, Node]](metadataSnapshot.aliveNodes.size + (if (existingUpsertBrokerState.isEmpty) 1 else 0))
    // insert references to existing alive brokers/nodes for ones that don't correspond to the upserted broker
    for ((existingBrokerId, existingBroker) <- metadataSnapshot.aliveBrokers) {
      if (existingBrokerId != upsertBrokerId) {
        newAliveBrokers(existingBrokerId) = existingBroker
      }
    }
    for ((existingBrokerId, existingListenerNameToNodeMap) <- metadataSnapshot.aliveNodes) {
      if (existingBrokerId != upsertBrokerId) {
        newAliveNodes(existingBrokerId) = existingListenerNameToNodeMap
      }
    }
    // add new alive broker/nodes for the upserted broker
    val nodes = new util.HashMap[ListenerName, Node]
    val endPoints = new ArrayBuffer[EndPoint]
    brokerRecord.endPoints().forEach { ep =>
      val listenerName = new ListenerName(ep.name())
      endPoints += new EndPoint(ep.host, ep.port, listenerName, SecurityProtocol.forId(ep.securityProtocol))
      nodes.put(listenerName, new Node(upsertBrokerId, ep.host, ep.port))
    }
    newAliveBrokers(upsertBrokerId) = Broker(upsertBrokerId, endPoints, Option(brokerRecord.rack))
    newAliveNodes(upsertBrokerId) = nodes.asScala
    metadataCacheBasis.metadataCache.logListenersNotIdenticalIfNecessary(newAliveNodes)

    val newMetadataCacheBasis = metadataCacheBasis.newBasis(
      MetadataSnapshot(metadataSnapshot.partitionStates, metadataSnapshot.controllerId, newAliveBrokers, newAliveNodes))
    newMetadataCacheBasis
  }
}
