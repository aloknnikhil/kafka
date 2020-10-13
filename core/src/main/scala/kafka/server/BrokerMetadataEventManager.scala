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

import java.util
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import kafka.metrics.KafkaMetricsGroup
import kafka.utils.ShutdownableThread
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.metadata.MetadataRecordType
import org.apache.kafka.common.protocol.ApiMessage
import org.apache.kafka.common.utils.Time

object BrokerMetadataEventManager {
  val BrokerMetadataEventThreadNamePrefix = "broker-"
  val BrokerMetadataEventThreadNameSuffix = "-metadata-event-thread"
  val EventQueueTimeMetricName = "EventQueueTimeMs"
  val EventQueueSizeMetricName = "EventQueueSize"
}

sealed trait Event {}

final case object StartupEvent extends Event
final case class MetadataEvent(apiMessages: List[ApiMessage]) extends Event
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
  def process(apiMessages: List[ApiMessage]): Unit
  // note the absence of processShutdown(); see ShutdownEvent above for why it does not exist.
}

class BrokerMetadataEventManager(config: KafkaConfig,
                                 initialBrokerMetadataBasis: BrokerMetadataBasis,
                                 time: Time,
                                 processors: util.EnumMap[MetadataRecordType, ApiMessageProcessor],
                                 eventQueueTimeTimeoutMs: Long = 300000) extends KafkaMetricsGroup with MetadataEventProcessor {

  val missingProcessors = MetadataRecordType.values().filter(v => processors.get(v) == null)
  if (!missingProcessors.isEmpty) {
    throw new IllegalArgumentException(s"Missing ApiMessage Processors: ${missingProcessors.mkString(",")}")
  }

  import BrokerMetadataEventManager._

  val blockingQueue = new LinkedBlockingQueue[QueuedEvent]
  val bufferQueue: util.Queue[QueuedEvent] = new util.ArrayDeque[QueuedEvent]
  // we need buffer queue size for a metric read in a separate thread, so keep the size in a thread-safe way
  @volatile var bufferQueueSize: Int = 0

  val thread = new BrokerMetadataEventThread(
    s"$BrokerMetadataEventThreadNamePrefix${config.brokerId}$BrokerMetadataEventThreadNameSuffix")

  var currentBasis: BrokerMetadataBasis = initialBrokerMetadataBasis

  // metrics
  private val eventQueueTimeHist = newHistogram(EventQueueTimeMetricName)
  newGauge(EventQueueSizeMetricName, () => blockingQueue.size + bufferQueueSize)

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
    blockingQueue.put(queuedEvent)
    queuedEvent
  }

  override def processStartup(): Unit = {
  }

  override def process(apiMessages: List[ApiMessage]): Unit = {
    try {
      trace(s"Handling metadata messages: $apiMessages")
      var basis = currentBasis
      apiMessages.foreach(message => {
        val processor = processors.get(MetadataRecordType.fromId(message.apiKey()))
        basis = processor.process(basis, message)
      })
      // only expose the changes at the end -- do not expose intermediate state
      basis.writeIfNecessary()
      currentBasis = basis
    } catch {
      case e: FatalExitError => throw e
      case e: Exception => handleError(apiMessages, e)
    }
  }

  def handleError(apiMessages: List[ApiMessage], e: Exception): Unit = {
    error(s"Error when handling metadata messages: $apiMessages", e)
  }

  class BrokerMetadataEventThread(name: String) extends ShutdownableThread(name = name, isInterruptible = false) {
    logIdent = s"[BrokerMetadataEventThread] "

    override def doWork(): Unit = {
      val dequeued: QueuedEvent = pollFromEventQueue()
      dequeued.event match {
        case StartupEvent => processStartup()
        case metadataEvent: MetadataEvent =>
          eventQueueTimeHist.update(time.milliseconds() - dequeued.enqueueTimeMs)
          try {
            process(metadataEvent.apiMessages)
          } catch {
            case e: Throwable => error(s"Uncaught error processing event: $metadataEvent", e)
          }
        case ShutdownEvent => // Ignore since it serves solely to wake us up and we weren't guaranteed to see it
      }
    }

    private def pollFromEventQueue(): QueuedEvent = {
      val bufferedEvent = bufferQueue.poll()
      if (bufferedEvent != null) {
        bufferQueueSize -= 1
        return bufferedEvent
      }
      val numBuffered = blockingQueue.drainTo(bufferQueue)
      if (numBuffered != 0) {
        // it's already 0, so don't write 0 again
        if (numBuffered != 1) {
          bufferQueueSize = numBuffered - 1
        }
        return bufferQueue.poll()
      }
      val hasRecordedValue = eventQueueTimeHist.count() > 0
      if (hasRecordedValue) {
        val event = blockingQueue.poll(eventQueueTimeTimeoutMs, TimeUnit.MILLISECONDS)
        if (event == null) {
          eventQueueTimeHist.clear()
          blockingQueue.take()
        } else {
          event
        }
      } else {
        blockingQueue.take()
      }
    }
  }
}
