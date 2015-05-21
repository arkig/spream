/*
 * Copyright 2015 Ark International Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark.{Logging, RangePartitioner}
import org.apache.spark.annotation.DeveloperApi
import spream.{PartitionedSeriesKey, PartitionedSeriesPartitioner}

// Add support for PartitionedSeriesPartitioner

class OrderedRDDFunctions2[K : Ordering : ClassTag,
  V: ClassTag,
  P <: Product2[K, V] : ClassTag] @DeveloperApi() (self: RDD[P])
  extends Logging with Serializable
{
  private val ordering = implicitly[Ordering[K]]

  def filterByRange2(lower: K, upper: K): RDD[P] = {

    def inRange(k: K): Boolean = ordering.gteq(k, lower) && ordering.lteq(k, upper)

    val rddToFilter: RDD[P] = self.partitioner match {
      case Some(rp: RangePartitioner[K, V]) => {
        val partitionIndicies = (rp.getPartition(lower), rp.getPartition(upper)) match {
          case (l, u) => Math.min(l, u) to Math.max(l, u)
        }
        PartitionPruningRDD.create(self, partitionIndicies.contains)
      }
      case Some(rp: PartitionedSeriesPartitioner[K]) => {
        val partitionIndicies = (rp.getPartitionTypesafe(lower), rp.getPartitionTypesafe(upper)) match {
          case (l, u) => Math.min(l, u) to Math.max(l, u)
        }
        PartitionPruningRDD.create(self, partitionIndicies.contains)
      }
      case _ =>
        self
    }
    rddToFilter.filter { case (k, v) => inRange(k) }
  }

}


class PartitionedSeriesRDDFunctions[K : Ordering : ClassTag,
V: ClassTag,
P <: Product2[PartitionedSeriesKey[K], V] : ClassTag] @DeveloperApi() (self: RDD[P])
  extends Logging with Serializable
{
  private val ordering = implicitly[Ordering[K]]

  def filterByRange2(lower: K, upper: K): RDD[P] = {

    def inRange(k: K): Boolean = ordering.gteq(k, lower) && ordering.lteq(k, upper)

    val rddToFilter: RDD[P] = self.partitioner match {
      case Some(rp: RangePartitioner[K, V]) => {
        val partitionIndicies = (rp.getPartition(lower), rp.getPartition(upper)) match {
          case (l, u) => Math.min(l, u) to Math.max(l, u)
        }
        PartitionPruningRDD.create(self, partitionIndicies.contains)
      }
      case Some(rp: PartitionedSeriesPartitioner[K]) => {
        val partitionIndicies = (rp.getPartitionTypesafe(lower), rp.getPartitionTypesafe(upper)) match {
          case (l, u) => Math.min(l, u) to Math.max(l, u)
        }
        PartitionPruningRDD.create(self, partitionIndicies.contains)
      }
      case _ =>
        self
    }
    rddToFilter.filter { case (k, v) => inRange(k.key) }
  }

}

