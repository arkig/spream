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

package spream.util

trait Util {

  def zipWith[K,V1,V2](map1: Map[K, V1], map2: Map[K, V2]): Iterable[(K, Option[V1], Option[V2])] = {
    for(key <- map1.keys ++ map2.keys)
    yield (key, map1.get(key), map2.get(key))
  }

  def outerJoinAndAggregate[K,V1,V2,V](map1: Map[K, V1], map2: Map[K, V2], op : (Option[V1],Option[V2]) => V): Map[K,V] = {
    Map.empty[K,V] ++ (for(key <- map1.keys ++ map2.keys)
    yield (key -> op(map1.get(key),map2.get(key))))
  }

  /**
   * Optimisation where V1s remain unchanged if no match.
   */
  def outerJoinAndAggregate2[K,V1,V2](map1: Map[K, V1], map2: Map[K, V2], op : (Option[V1],V2) => V1): Map[K,V1] =
    map1 ++ map2.map { case (k,v) => k -> op(map1.get(k),v)}


  /**
   * Optimisation where V1s remain unchanged if no match.
   */
  def outerJoinAndAggregate3[K,V](map1: Map[K, V], map2: Map[K, V], op : (V,V) => V): Map[K,V] =
    map1 ++ map2.map { case (k,v) => k -> map1.get(k).map(op(_,v)).getOrElse(v)}

}
