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