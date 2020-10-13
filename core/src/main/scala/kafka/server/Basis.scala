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
 * The ability to retrieve or write a value.
 *
 * This is appropriate to use when it is the only write path to the value.
 *
 * @tparam T the value type
 */
trait ValueHolder[T] {
  def retrieveValue(): T

  def writeValue(value: T): Unit
}

/**
 * A stable basis represented by a single value.  It is assumed that retrieving a value can be relatively expensive
 * (e.g. it may require a read lock), so the value is retrieved once on-demand if/when required.
 * We can act upon this basis to create a new value/basis, and we can write the new value back to the destination
 * on-demand.  It is assumed that writing a value may be expensive (e.g. it may require a lock/synchronization),
 * so each instance keeps track of whether the value needs to be written or not.
 *
 * This is appropriate to use when it is the only write path to the value.
 *
 * @param valueHolder the source/destination for the value
 * @param valueToWrite the new value to write
 * @tparam T the value type
 */
class Basis[T](valueHolder: ValueHolder[T], valueToWrite: Option[T] = None) {
  private var value: Option[T] = valueToWrite
  private val hasDifferentValue = value.isDefined
  private var written = false

  def getValue(): T = {
    // retrieve if necessary
    if (value.isEmpty) {
      value = Some(valueHolder.retrieveValue())
    }
    value.get
  }

  def newBasis(valueToWrite: T): Basis[T] = {
    // create and return a new basis containing the new value; it will write that value to the destination on-demand
    new Basis(valueHolder, Some(valueToWrite))
  }

  def writeIfNecessary(): Unit = {
    // only write if we have a different value than the one that is already held and we haven't written that value yet
    if (hasDifferentValue && !written) {
      value.foreach(v => valueHolder.writeValue(v))
      written = true
    }
  }
}
