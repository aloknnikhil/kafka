package kafka.server

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import kafka.coordinator.group.GroupCoordinator
import kafka.metrics.KafkaMetricsGroup
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.ShutdownableThread
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.metadata.{AccessControlRecord, BrokerRecord, ConfigRecord, IsrChangeRecord, PartitionRecord, TopicRecord}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.raft.RaftMessage

object BrokerMetadataEventManager {
  val BrokerMetadataEventThreadNamePrefix = "broker-"
  val BrokerMetadataEventThreadNameSuffix = "-metadata-event-thread"
  val EventQueueTimeMetricName = "EventQueueTimeMs"
  val EventQueueSizeMetricName = "EventQueueSize"
}

sealed trait Event {}

final case object StartupEvent extends Event
final case class MetadataEvent(metadataMessage: RaftMessage) extends Event
/*
 * ShutdownEvent is necessary for the case when the event thread is blocked in queue.take() -- this event wakes it up.
 * Otherwise, this event has no other semantic meaning, it is not guaranteed to be seen by the processing thread,
 * and it cannot be relied upon to initiate any other functionality; it exists solely for the wakeup case mentioned.
 */
final case object ShutdownEvent extends Event

class QueuedEvent(val event: Event, val enqueueTimeMs: Long) {
  override def toString: String = {
    s"QueuedEvent(event=$event, enqueueTimeMs=$enqueueTimeMs)"
  }
}

trait MetadataEventProcessor {
  def processStartup(): Unit
  def process(metadataMessage: RaftMessage): Unit
  // note the absence of processShutdown(); see ShutdownEvent above for why it does not exist.
}

class BrokerMetadataEventManager(config: KafkaConfig,
                                 replicaManager: ReplicaManager,
                                 groupCoordinator: GroupCoordinator,
                                 metadataCache: MetadataCache,
                                 quotas: QuotaManagers,
                                 clusterId: String,
                                 time: Time,
                                 overrideProcessor: Option[MetadataEventProcessor] = None,
                                 eventQueueTimeTimeoutMs: Long = 300000) extends KafkaMetricsGroup with MetadataEventProcessor {

  import BrokerMetadataEventManager._

  val queue = new LinkedBlockingQueue[QueuedEvent]

  val thread = new BrokerMetadataEventThread(
    s"$BrokerMetadataEventThreadNamePrefix${config.brokerId}$BrokerMetadataEventThreadNameSuffix")

  val processor: MetadataEventProcessor = overrideProcessor.getOrElse(this)

  // metrics
  private val eventQueueTimeHist = newHistogram(EventQueueTimeMetricName)
  newGauge(EventQueueSizeMetricName, () => queue.size)

  def start(): Unit = {
    put(StartupEvent)
    thread.start()
  }

  def close(): Unit = {
    try {
      thread.initiateShutdown()
      put(ShutdownEvent) // wake up the thread in case it is blocked on queue.take()
      thread.awaitShutdown()
    } finally {
      removeMetric(EventQueueTimeMetricName)
      removeMetric(EventQueueSizeMetricName)
    }
  }

  def put(event: Event): QueuedEvent = {
    val queuedEvent = new QueuedEvent(event, time.milliseconds())
    queue.put(queuedEvent)
    queuedEvent
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

  override def processStartup(): Unit = {
  }

  override def process(metadataMessage: RaftMessage): Unit = {
    try {
      trace(s"Handling metadata message:$metadataMessage")
      val data = metadataMessage.data
      data match {
        case brokerRecord: BrokerRecord => handleBrokerRecord(brokerRecord)
//        case fenceBrokerRecord: FenceBrokerRecord => handleFenceBrokerRecord(fenceBrokerRecord)
        case topicRecord: TopicRecord => handleTopicRecord(topicRecord)
//        case removeTopicRecord: RemoveTopicRecord => handleRemoveTopicRecord(removeTopicRecord)
        case partitionRecord: PartitionRecord => handlePartitionRecord(partitionRecord)
        case configRecord: ConfigRecord => handleConfigRecord(configRecord)
        case isrChangeRecord: IsrChangeRecord => handleIsrChangeRecord(isrChangeRecord)
        case accessControlRecord: AccessControlRecord => handleAccessControlRecord(accessControlRecord)
//        case delegationTokenRecord: DelegationTokenRecord => handleDelegationTokenRecord(delegationTokenRecord)
//        case scramRecord: ScramRecord => handleScramRecord(scramRecord)
//        case featureLevelRecord: FeatureLevelRecord => handleFeatureLevelRecord(featureLevelRecord)
        case _ => handleUnexpected(metadataMessage)
      }
    } catch {
      case e: FatalExitError => throw e
      case e: Exception => handleError(metadataMessage, e)
    }
  }

  private def handleBrokerRecord(brokerRecord: BrokerRecord): Unit = {
    replicaManager.updateMetadataCache(brokerRecord)
  }

//  private def handleFenceBrokerRecord(fenceBrokerRecord: FenceBrokerRecord): Unit = {
//    throw new UnsupportedOperationException(s"Unimplemented: $fenceBrokerRecord") // TODO: implement-me
//  }

  private def handleTopicRecord(topicRecord: TopicRecord): Unit = {
    throw new UnsupportedOperationException(s"Unimplemented: $topicRecord") // TODO: implement-me
  }

//  private def handleRemoveTopicRecord(removeTopicRecord: RemoveTopicRecord): Unit = {
//    throw new UnsupportedOperationException(s"Unimplemented: $removeTopicRecord") // TODO: implement-me
//  }

  private def handlePartitionRecord(partitionRecord: PartitionRecord): Unit = {
    throw new UnsupportedOperationException(s"Unimplemented: $partitionRecord") // TODO: implement-me
  }

  private def handleConfigRecord(configRecord: ConfigRecord): Unit = {
    throw new UnsupportedOperationException(s"Unimplemented: $configRecord") // TODO: implement-me
  }

  private def handleIsrChangeRecord(isrChangeRecord: IsrChangeRecord): Unit = {
    throw new UnsupportedOperationException(s"Unimplemented: $isrChangeRecord") // TODO: implement-me
  }

  private def handleAccessControlRecord(accessControlRecord: AccessControlRecord): Unit = {
    throw new UnsupportedOperationException(s"Unimplemented: $accessControlRecord") // TODO: implement-me
  }

//  private def handleDelegationTokenRecord(delegationTokenRecord: DelegationTokenRecord): Unit = {
//    throw new UnsupportedOperationException(s"Unimplemented: delegationTokenRecord") // TODO: implement-me
//  }

//  private def handleScramRecord(scramRecord: ScramRecord): Unit = {
//    throw new UnsupportedOperationException(s"Unimplemented: scramRecord") // TODO: implement-me
//  }

//  private def handleFeatureLevelRecord(featureLevelRecord: FeatureLevelRecord): Unit = {
//    throw new UnsupportedOperationException(s"Unimplemented: featureLevelRecord") // TODO: implement-me
//  }

  def handleUnexpected(metadataMessage: RaftMessage): Unit = {
    error(s"Ignoring unexpected metadata message=$metadataMessage")
  }

  def handleError(metadataMessage: RaftMessage, e: Exception): Unit = {
    error(s"Error when handling metadata message=$metadataMessage", e)
  }

  class BrokerMetadataEventThread(name: String) extends ShutdownableThread(name = name, isInterruptible = false) {
    logIdent = s"[BrokerMetadataEventThread] "

    override def doWork(): Unit = {
      val dequeued: QueuedEvent = pollFromEventQueue()
      dequeued.event match {
        case StartupEvent => processor.processStartup()
        case metadataEvent: MetadataEvent =>
          eventQueueTimeHist.update(time.milliseconds() - dequeued.enqueueTimeMs)
          try {
            processor.process(metadataEvent.metadataMessage)
          } catch {
            case e: Throwable => error(s"Uncaught error processing event $metadataEvent", e)
          }
        case ShutdownEvent => // Ignore since it serves solely to wake us up and we weren't guaranteed to see it
      }
    }
  }
}
