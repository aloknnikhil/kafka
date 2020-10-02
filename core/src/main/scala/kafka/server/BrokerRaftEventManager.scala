package kafka.server

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import kafka.coordinator.group.GroupCoordinator
import kafka.metrics.KafkaMetricsGroup
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.ShutdownableThread
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.message.UpdateMetadataRequestData
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, ApiMessage}
import org.apache.kafka.common.requests.UpdateMetadataRequest
import org.apache.kafka.common.utils.Time
import org.apache.kafka.raft.RaftMessage

object BrokerRaftEventManager {
  val BrokerRaftEventThreadName = "broker-raft-event-thread"
  val EventQueueTimeMetricName = "EventQueueTimeMs"
  val EventQueueSizeMetricName = "EventQueueSize"
}

trait RaftEventProcessor {
  def startupBeforeAnyProcessing(): Unit
  def process(event: RaftMessage): Unit
}

class QueuedEvent(val event: RaftMessage, val enqueueTimeMs: Long) {
  override def toString: String = {
    s"QueuedEvent(event=$event, enqueueTimeMs=$enqueueTimeMs)"
  }
}

case object StartupBeforeAnyProcessing extends RaftMessage {
  override def correlationId(): Int = -1
  override def data(): ApiMessage = null
}

case object ShutdownEventThread extends RaftMessage {
  override def correlationId(): Int = -1
  override def data(): ApiMessage = null
}

class BrokerRaftEventManager(replicaManager: ReplicaManager,
                             groupCoordinator: GroupCoordinator,
                             metadataCache: MetadataCache,
                             quotas: QuotaManagers,
                             clusterId: String,
                             time: Time,
                             optionalProcessor: Option[RaftEventProcessor] = None,
                             eventQueueTimeTimeoutMs: Long = 300000) extends KafkaMetricsGroup with RaftEventProcessor {

  import BrokerRaftEventManager._

  val queue = new LinkedBlockingQueue[QueuedEvent]

  val thread = new BrokerRaftEventThread(BrokerRaftEventThreadName)

  val processor: RaftEventProcessor = optionalProcessor.getOrElse(this)

  // metrics
  private val eventQueueTimeHist = newHistogram(EventQueueTimeMetricName)
  newGauge(EventQueueSizeMetricName, () => queue.size)

  def start(): Unit = thread.start()

  def close(): Unit = {
    try {
      thread.initiateShutdown()
      thread.awaitShutdown()
    } finally {
      removeMetric(EventQueueTimeMetricName)
      removeMetric(EventQueueSizeMetricName)
    }
  }

  def put(event: RaftMessage): QueuedEvent = {
    val queuedEvent = new QueuedEvent(event, time.milliseconds())
    queue.put(queuedEvent)
    queuedEvent
  }

  class BrokerRaftEventThread(name: String) extends ShutdownableThread(name = name, isInterruptible = false) {
    logIdent = s"[BrokerRaftEventThread] "

    override def doWork(): Unit = {
      val dequeued = pollFromEventQueue()
      dequeued.event match {
        case ShutdownEventThread => // The shutting down of the thread has been initiated at this point. Ignore this event.
        case StartupBeforeAnyProcessing => processor.startupBeforeAnyProcessing()
        case raftMessage =>
          eventQueueTimeHist.update(time.milliseconds() - dequeued.enqueueTimeMs)
          try {
            processor.process(raftMessage)
          } catch {
            case e: Throwable => error(s"Uncaught error processing event $raftMessage", e)
          }
      }
    }
  }

  private def pollFromEventQueue(): QueuedEvent = {
    val hasRecordedValue = eventQueueTimeHist.count() > 0
    if (hasRecordedValue) {
      val event = queue.poll(eventQueueTimeTimeoutMs, TimeUnit.MILLISECONDS)
      if (event == null) {
        eventQueueTimeHist.clear()
        queue.take()
      } else {
        event
      }
    } else {
      queue.take()
    }
  }

  override def startupBeforeAnyProcessing(): Unit = {
  }

  override def process(event: RaftMessage): Unit = {
    try {
      trace(s"Handling event:$event")
      val data = event.data
      data.apiKey match {
        case ApiKeys.UPDATE_METADATA.id => handleUpdateMetadataRequest(event)
        case _ => handleUnexpected(event)
      }
    } catch {
      case e: FatalExitError => throw e
      case e: Throwable => handleError(event, e)
    }
  }

  def handleUpdateMetadataRequest(event: RaftMessage): Unit = {
    val data = event.data.asInstanceOf[UpdateMetadataRequestData]
    val version = data.highestSupportedVersion
    val deletedPartitions = replicaManager.maybeUpdateMetadataCache(event.correlationId, data, version)
    if (deletedPartitions.nonEmpty)
      groupCoordinator.handleDeletedPartitions(deletedPartitions)
    quotas.clientQuotaCallback.foreach { callback =>
      val listenerNameForQuotas = new ListenerName("whatDoWeUseHere")
      if (callback.updateClusterMetadata(metadataCache.getClusterMetadata(clusterId, listenerNameForQuotas))) {
        quotas.fetch.updateQuotaMetricConfigs()
        quotas.produce.updateQuotaMetricConfigs()
        quotas.request.updateQuotaMetricConfigs()
        quotas.controllerMutation.updateQuotaMetricConfigs()
      }
    }
    if (replicaManager.hasDelayedElectionOperations) {
      UpdateMetadataRequest.partitionStates(data, version).forEach { partitionState =>
        val tp = new TopicPartition(partitionState.topicName, partitionState.partitionIndex)
        replicaManager.tryCompleteElection(TopicPartitionOperationKey(tp))
      }
    }
  }

  def handleUnexpected(event: RaftMessage): Unit = {
    error(s"Ignoring unexpected event=$event")
  }

  def handleError(event: RaftMessage, e: Throwable): Unit = {
    error(s"Error when handling event=$event", e)
  }
}
