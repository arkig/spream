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

