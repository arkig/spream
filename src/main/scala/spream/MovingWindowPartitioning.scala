package spream

import scala.reflect.ClassTag

import scala.collection.mutable

import org.apache.spark.rdd._
import org.apache.spark.{RangePartitioning, Partitioner}

object PartitionLocation {
  sealed trait EnumVal
  case object Past extends EnumVal
  case object Current extends EnumVal
  case object Future extends EnumVal
}

/**
 * Given a series indexed by K, it can be parallelised across partitions to a series indexed by PartitionedSeriesKey[K].
 * @param partition the id of the partition containing this. This is used to implement efficient partitioning and
 *                  probably should not be used by client code.
 * @param key the underlying key for the series.
 * @param location whether key is part of this current partition, or duplicated from a
 *                 previous (past) or subsequent (future) partition in order to allow moving window computations.
 * @tparam K
 */
case class PartitionedSeriesKey[K : Ordering : Numeric : ClassTag](partition : Int, key : K, location : PartitionLocation.EnumVal)

object PartitionedSeriesKey {
  implicit def ordering[K : Ordering] : Ordering[PartitionedSeriesKey[K]] = Ordering.by(x => (x.partition,x.key))
}


//TODO write this like PairRDDFunctions and have an implicit conversion, so can convert ordered RDDs to this easily.
//TODO prevent the currently required type specification required when calling these functions (the above may help)
//TODO type the resulting RDD, e.g. SeriesRDD
//TODO implement faster ops on this, because we know it's a type of range partitioning so filtering can be made fast in the same way.
//TODO consider writing this ias a subclass of RangePartitioner somehow (though note the duplication cannot be done by getPartition).

/**
 * In order to parallelise moving window computations on series, it is necessary to duplicate data so that there
 * is an overlap between partitions.
 * Specifically, a partition p must have enough data from partition p-1 and possibly earlier (and p+1 and possibly
 * later) to fill the past (and future) windows.
 */
object MovingWindowPartitioning {

  type IntervalSets[K] = List[(Option[K],Set[(Int, PartitionLocation.EnumVal)])]


  /**
   * Given rangeBounds and desired past and future window widths, determine the (Past,Current,Future) labelling
   * that should be applied within each interval.
   * This allows efficient application of labeling.
   * @param rangeBounds must be sorted according to K
   * @param pastWindow
   * @param futureWindow
   * @return A list of (upper bound, labels), where labels are to be applied until the bound is hit.
   */
  def intervalSets[K : Ordering : Numeric](rangeBounds : Array[K], pastWindow : K, futureWindow : K): IntervalSets[K] = {

    var numeric = implicitly[Numeric[K]]

    val Start = 's'
    val End = 'e'

    val allBounds = rangeBounds.zipWithIndex.flatMap{ case (ub,p) =>
      (p,PartitionLocation.Current,End,ub) ::
        (p,PartitionLocation.Future,Start,ub) ::
        (p,PartitionLocation.Future,End,numeric.plus(ub,futureWindow)) ::
        (p+1,PartitionLocation.Past,Start,numeric.minus(ub,pastWindow)) ::
        (p+1,PartitionLocation.Past,End,ub) ::
        (p+1,PartitionLocation.Current,Start,ub) :: Nil : List[(Int,PartitionLocation.EnumVal,Char,K)]
    }.sortBy(_._4)

    type ACC = IntervalSets[K]
    val empty : ACC = Nil

    //Anything lower than first split is in "this" of first partition
    //We start with bound being lower
    val initial : ACC = (Option.empty[K],Set((0,PartitionLocation.Current : PartitionLocation.EnumVal))) :: Nil

    val intervalSets: ACC = allBounds.foldLeft(initial){
      case (acc,(p,what,se,bound)) =>
        val currBound = acc.head._1
        val currSet = acc.head._2
        val entry = (p,what)
        val ns = se match {
          case Start => currSet + entry
          case End => currSet - entry
        }
        //Note this reverses the list.
        if (currBound.map(_ == bound).getOrElse(false))
          (Some(bound), ns) :: acc.tail
        else
          (Some(bound), ns) :: acc
    }.foldLeft((Option.empty[K],empty)){
      //Note this reverses again. Here, we're making the bound upper, rather than lower.
      case ((prevBound,res),(bound,set)) => (bound,(prevBound,set) :: res)
    }._2

    require(rangeBounds.size+1 <= intervalSets.size)

    intervalSets
  }


  //TODO can we enforce rdd ordered?
  /**
   * Applies duplication and labelling as defined by intervalSets in a streaming fashion over each partition.
   * @param rdd ordered by K
   * @param intervalSets
   * @return
   */
  def duplicateToIntervals[K : Ordering : Numeric : ClassTag, V : ClassTag, P <: Product2[K,V] : ClassTag](
             rdd: RDD[P], intervalSets : IntervalSets[K]): RDD[Product2[PartitionedSeriesKey[K], V]] = {
    def f(it : Iterator[P]) = new DuplicateToIntervalsIterator[K,V,P](it,intervalSets)
    rdd.mapPartitions(f,true)
  }

  /**
   * Apply the pre-computed partitioning.
   * @param rdd
   * @param rangeBounds used to pre-label partitions
   * @return
   */
  def applyPartitioning[K : Ordering : Numeric : ClassTag, V : ClassTag, P <: Product2[PartitionedSeriesKey[K],V] : ClassTag](
             rdd: RDD[P], rangeBounds : Array[K], ascending : Boolean): RDD[(PartitionedSeriesKey[K], V)] = {
    val f = new OrderedRDDFunctions[PartitionedSeriesKey[K], V, P](rdd)
    f.repartitionAndSortWithinPartitions(new PartitionedSeriesPartitioner(rangeBounds, ascending))
  }

  /**
   * Convert an RDD of (K,V) ordered by K (a series) into an RDD with appropriate overlaps and labels to allow
   * moving window computations to be applied. Data is distributed roughly evenly across the resulting partitions.
   * @param rdd
   * @param pastWidth defines window width in which past events will be made available in each partition.
   * @param futureWidth
   * @param partitions number of partitions desired
   * @tparam K
   * @tparam V
   * @tparam P
   * @return
   */
  def movingWindowPartitioned[K : Ordering : Numeric : ClassTag, V : ClassTag, P <: Product2[K,V] : ClassTag](
             rdd: RDD[P], pastWidth : K, futureWidth : K, partitions : Int): RDD[(PartitionedSeriesKey[K], V)] = {
    val rb = RangePartitioning.rangeBounds[K,V](rdd,partitions)
    val is = intervalSets(rb,pastWidth,futureWidth)
    val d = duplicateToIntervals[K,V,P](rdd,is)
    applyPartitioning(d,rb,true)
  }


}

//TODO extend sRangPartitioner, and if not PartitionedSeriesKey, delegate to that... should work ??

class PartitionedSeriesPartitioner[K : Ordering : ClassTag](val rangeBounds : Array[K], val ascending : Boolean) extends Partitioner { //RangePartitioner[K,Unit](rangeBounds.length+1,null,true) {

  private var ordering = implicitly[Ordering[K]]

  override def numPartitions: Int = rangeBounds.length + 1

  // These are used to filter by range
  def getPartitionTypesafe(key: K): Int = doGetPartition(key)
  def getPartitionTypesafe(key: PartitionedSeriesKey[K]): Int = key.partition

  // Used to perform partitioning. No control over the API to improve this...
  override def getPartition(key: Any): Int =
    getPartitionTypesafe(key.asInstanceOf[PartitionedSeriesKey[K]])

  private var binarySearch: ((Array[K], K) => Int) =
    org.apache.spark.util.CollectionsUtils.makeBinarySearch[K]

  // Unfortunately RangePartitioner isn't written in a way where you can inherit from it, so the following
  // is almost a cut-and-paste of functionality there.
  private def doGetPartition(k: K): Int = {
    var partition = 0
    if (rangeBounds.length <= 128) {
      // If we have less than 128 partitions naive search
      while (partition < rangeBounds.length && ordering.gt(k, rangeBounds(partition))) {
        partition += 1
      }
    } else {
      // Determine which binary search method to use only once.
      partition = binarySearch(rangeBounds, k)
      // binarySearch either returns the match location or -[insertion point]-1
      if (partition < 0) {
        partition = -partition-1
      }
      if (partition > rangeBounds.length) {
        partition = rangeBounds.length
      }
    }
    if (ascending) {
      partition
    } else {
      rangeBounds.length - partition
    }
  }

}

/**
 * Applies interval mapping in a streaming fashion.
 * @param it
 * @param intervalSets
 * @tparam K
 * @tparam V
 * @tparam P
 */
class DuplicateToIntervalsIterator[K : Ordering : Numeric : ClassTag,V,P <: Product2[K,V] : ClassTag](
         it : Iterator[P], intervalSets : List[(Option[K],Set[(Int, PartitionLocation.EnumVal)])]) extends Iterator[Product2[PartitionedSeriesKey[K],V]]() {

  private val ordering = implicitly[Ordering[K]]
  private val queue = new mutable.Queue[Product2[PartitionedSeriesKey[K],V]]()
  private val currentIntervals: mutable.Queue[(Option[K], Set[(Int, PartitionLocation.EnumVal)])] = mutable.Queue() ++ intervalSets
  private var prev : Option[P] = None //Only used to enforce ordering

  //Skip to correct interval for k (interval is closed below and open above).
  //In future, could be worth changing queue to an Array and then binary searching it to get to the initial location.
  //Note: Will probably make little difference in practice though as it will only happen once per partition.
  def skipTo(k : K) =
    while (currentIntervals.head._1.map(ordering.gteq(k, _)).getOrElse(false))
      currentIntervals.dequeue()

  override def hasNext: Boolean = !queue.isEmpty || it.hasNext

  override def next() = {

    if (queue.isEmpty) {

      val n: P = it.next()
      require(prev.map(p => ordering.gteq(n._1,p._1)).getOrElse(true),
        "Out of order: "+ n._1 + " not >= " + prev.get._1)

      skipTo(n._1)

      // Assign n to all currently active intervals
      queue ++= currentIntervals.head._2.map{ case (partition,what) =>
        (PartitionedSeriesKey(partition,n._1,what),n._2)
      }

      prev = Some(n)
    }
    queue.dequeue()
  }
}

