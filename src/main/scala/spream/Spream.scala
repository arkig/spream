package spream

import scala.reflect.ClassTag
import org.apache.spark.rdd.{MoreRDDFunctions, RDD, OrderedRDDFunctions2, PartitionedSeriesRDDFunctions}

/**
 * Use import spream.Spream._ in applications to enable these conversions, so you can use
 * the more efficient filterByRange2.
 */
object Spream {

  implicit def rddToMoreRDDFunctions[V](rdd: RDD[V])(implicit vt: ClassTag[V]): MoreRDDFunctions[V] =
    new MoreRDDFunctions(rdd)

  implicit def rddToOrderedRDDFunctions2[K : Ordering : ClassTag, V: ClassTag](rdd: RDD[(K, V)])
  : OrderedRDDFunctions2[K, V, (K, V)] = {
    new OrderedRDDFunctions2[K, V, (K, V)](rdd)
  }

  implicit def rddToPartitionedSeriesRDDFunctions2[K : Ordering : ClassTag, V: ClassTag](rdd: RDD[(PartitionedSeriesKey[K], V)])
  : PartitionedSeriesRDDFunctions[K, V, (PartitionedSeriesKey[K], V)] = {
    new PartitionedSeriesRDDFunctions[K, V, (PartitionedSeriesKey[K], V)](rdd)
  }

}