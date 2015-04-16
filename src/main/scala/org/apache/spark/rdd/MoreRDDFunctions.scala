package org.apache.spark.rdd

import scala.reflect.ClassTag
import org.apache.spark.Logging
import scala.util.Random

class MoreRDDFunctions[V](self: RDD[V])(implicit vt: ClassTag[V])
  extends Logging with Serializable
{

  def shuffle(numPartitions: Int = self.partitions.size) : RDD[V] = {
    self.mapPartitionsWithIndex{ case (partition,it) =>
      val r = new Random(partition*43)
      it.map((r.nextDouble(),_))
    }.sortBy(_._1,numPartitions = numPartitions).map(_._2)
  }
}