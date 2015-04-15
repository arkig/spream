package org.apache.spark.rdd

import scala.reflect.ClassTag
import spream.PartitionedSeriesKey

//TODO find a better place fot this.

object RDD2 {

  implicit def rddToOrderedRDDFunctions2[K : Ordering : ClassTag, V: ClassTag](rdd: RDD[(K, V)])
  : OrderedRDDFunctions2[K, V, (K, V)] = {
    new OrderedRDDFunctions2[K, V, (K, V)](rdd)
  }

  implicit def rddToPartitionedSeriesRDDFunctions2[K : Ordering : ClassTag, V: ClassTag](rdd: RDD[(PartitionedSeriesKey[K], V)])
  : PartitionedSeriesRDDFunctions[K, V, (PartitionedSeriesKey[K], V)] = {
    new PartitionedSeriesRDDFunctions[K, V, (PartitionedSeriesKey[K], V)](rdd)
  }

}