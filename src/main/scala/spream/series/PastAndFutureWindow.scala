package spream.series

import scalaz.stream.Process1
import scala.Some
import scala.annotation.tailrec
import spream.stream.UsefulProcessors
import spream.util.Util
import scala.reflect.ClassTag
import spream.{PartitionLocation, PartitionedSeriesKey}
import scala.collection.SeqView


/**
 * Maintains a window split into past (including 'now') and future (excluding 'now) components.
 * Ordering is from now. That is, past is decreasing by K (first is now), while future is ascending.
 * It is useful to think of K as a timestamp.
 * Subclasses can define how the past and future is bounded.
 *
 * Represented by an underlying vector increasing in K.
 *
 * Note: Vector is used here as we don't get access to underlying red black
 * tree / can't get closed ranges / (before,after) a particular time with a TreeMap implementation.
 *
 * TODO nowIndex should probably not be an option. Instead, use -1 to signal unset.
 *
 * TODO consider whether this should be Serializable. Must be careful in allowing this because the whole point of using
 * an immutable vector is that previous versions of it are effectively reused in previous windows, so a stream of these things
 * uses less memory that one might first suspect. Serialization is required for sending prototypes over the wire (for asProcess1 etc),
 * but it then also allows an RDD of windows to be serialized - which we don't need to do (and indicates inefficient usage - like RDD.map
 * rather than process1.map).
 */
abstract class BoundedPastAndFutureWindow[K : Ordering : Numeric : ClassTag, V : ClassTag, P <: Product2[K,V] : ClassTag](
          nowIndex : Option[Int] = None, window : Vector[P] = Vector.empty) extends Serializable
{

  protected val numeric = implicitly[Numeric[K]]

  def now() = BoundedPastAndFutureWindow.now[K,V,P](nowIndex,window)

  def nextIndex() = BoundedPastAndFutureWindow.nextIndex[K,V,P](nowIndex,window)

  def start() = window.headOption

  def end() = window.lastOption

  def future(): Option[SeqView[P, Vector[P]]] = for {
    npp <- nextIndex
  } yield window.view(npp,window.size)

  def past(): Option[SeqView[P, Vector[P]]] = nowIndex.map { ni =>
    val npp = ni + 1 //  nextIndex.getOrElse(window.size)
    window.view(0,npp).reverse
  }

  private def binarySearch(key: K, low : Int = 0, high : Int = window.length-1): Int = {

    @tailrec
    def binarySearch(low: Int, high: Int): Int = {
      if (low <= high) {
        val middle = low + (high - low) / 2

        if (window(middle)._1 == key)
          middle
        else if (numeric.lt(window(middle)._1,key))
          binarySearch(middle + 1, high)
        else
          binarySearch(low, middle - 1)
      } else
        -(low + 1)
    }

    binarySearch(low,high)
  }

  def atIndex(offset : Int) =
    nowIndex.flatMap{ case n =>
      val i = n + offset
      if (i >= 0 && i < window.length)
        Some(window(i))
      else None
    }

  def atPastIndex(offset : Int) = {
    assert(offset >= 0)
    atIndex(-offset)
  }


  def atOffset(offset : K) = indexAtOffset(offset).map(window)

  /**
   * @param offset
   * @return the most recent (at now + offset or before) window index
   */
  def indexAtOffset(offset : K) = {
    nowIndex.flatMap { n =>
      val t = numeric.plus(window(n)._1,offset)
      val found = if (numeric.gt(offset,numeric.zero))
        nextIndex().map(i => binarySearch(t,i))
      else
        Some(binarySearch(t,0,n))
      found.flatMap{ case i =>
        if (i >= window.length) None
        else if (i >= 0) //hit
          Some(i) //Some(window(i))
        else if (-i-2 >= window.length - 1) {
          None //beyond range (future), and don't want to return the latest value we have because there may be another we haven't seen yet,
          //which means when that gets added, the result would change
        }
        else if (-i-2 < 0)
          None //beyond range (past)
        else //between, return index of closest in the past
          Some(-i-2) //Some(window(-i-2))
      }
    }
  }

  /**
   *
   * @param offset offset measure from now into the past, >= 0
   * @return the most recent value known at this offset, as well as the index into past() that this corresponds to.
   */
  def atPastOffset(offset : K) = {
    assert(numeric.gteq(offset,numeric.zero))
    indexAtOffset(numeric.negate(offset)).map(i => (window(i),nowIndex.get - i))
  }

}

object BoundedPastAndFutureWindow {

  def now[K : Ordering : Numeric : ClassTag, V : ClassTag, P <: Product2[K,V] : ClassTag](nowIndex : Option[Int], window : Vector[P]) =
    nowIndex.map(window(_))

  /**
   * nowIndex starts as unset (None), which is valid. In contrast, note that when this returns None,
   * this signals there is no valid next index.
   * @return the next index, if there is one.
   */
  def nextIndex[K : Ordering : Numeric : ClassTag, V : ClassTag, P <: Product2[K,V] : ClassTag](nowIndex : Option[Int], window : Vector[P]) =
    nowIndex match {
      case None if window.size > 0 => Some(0)
      case Some(n) if (n+1 < window.length) => Some(n+1)
      case default => None
    }

}


/**
 * Moving window bounded by interval(s) specified in terms of K.
 * Note: In order to guarantee the behaviour of minFutureWidth (when non-zero), there must always be one entry in future()
 * that is greater than moved().now()._1 + minFutureWidth in the future. Note the moved(), as future() starts on the next entry after now().
 * Note: similar for past (if relying on pastFull), except that past() contains now().
 *
 * TODO consider measuring future() span from now()! makes more sense if doing atFutureOffset etc...
 * TODO make the (current) crossing of the boundary (required for last-known-value interpolation) optional.
 *
 * @param minPastWidth minimum span of past(). Once filled, past() will be at least as long as required so that atPastOffset(minPastWidth) will return an entry
 *                     closest (from below) or equal to now()._1 - minPastWidth. Past can be shorter if no values have yet been seen that
 *                     would fall into past - but in that situation, pastFull is false.
 * @param minFutureWidth minimum span of future(). future() will be the minimum length so that atOffset(minFutureWidth) will return the entry
 *                       closest (from below) or equal to moved().now()._1 + minFutureWidth.
 * @param nowIndex
 * @param window
 * @tparam K
 * @tparam V
 * @tparam P
 */
case class ValueBoundedPastAndFutureWindow[K : Ordering : Numeric : ClassTag, V : ClassTag, P <: Product2[K,V] : ClassTag](
       minPastWidth : K, minFutureWidth : K, nowIndex : Option[Int] = None, window : Vector[P] = Vector.empty, pastFull : Boolean = false)
  extends BoundedPastAndFutureWindow[K,V,P](nowIndex,window)
{
  require(numeric.gteq(minPastWidth,numeric.zero))
  require(numeric.gteq(minFutureWidth,numeric.zero))

  /**
   * Just appends a new record
   */
  def updated(r : P): ValueBoundedPastAndFutureWindow[K, V, P] = copy(window = (this.window :+ r))

  def moved(): Option[ValueBoundedPastAndFutureWindow[K, V, P]] = ValueBoundedPastAndFutureWindow.moved[K,V,P](minPastWidth,minFutureWidth)(nowIndex,window, pastFull)
    .map{ case (i,w,pf) => copy(nowIndex = i, window = w, pastFull = pf)}


  def movedUntil(target : K): (Option[ValueBoundedPastAndFutureWindow[K, V, P]], Option[ValueBoundedPastAndFutureWindow[K, V, P]]) = {
    val (closest,found) = ValueBoundedPastAndFutureWindow.moveUntil[K,V,P](target, nowIndex, window, pastFull,
      ValueBoundedPastAndFutureWindow.moved[K,V,P](minPastWidth, minFutureWidth) _)
    (closest.map(x => copy(nowIndex = x._1, window = x._2)),
      found.map(x => copy(nowIndex = x._1, window = x._2)))
  }



}



object ValueBoundedPastAndFutureWindow {

  // TODO consider changing this so that will output a collection of these instead. can do this because logic in canMove should be able to determine how many steps possible.

  /**
   * Moves now ahead one, if possible.
   * Note this may be called multiple times.
   */
  def moved[K : Ordering : Numeric : ClassTag, V : ClassTag, P <: Product2[K,V] : ClassTag]( minPastWidth : K, minFutureWidth : K)(nowIndex : Option[Int], window : Vector[P], pastFull : Boolean): Option[(Some[Int], Vector[P], Boolean)] = {

    val numeric = implicitly[Numeric[K]]

    val canMove = if (numeric.gt(minFutureWidth,numeric.zero)) {

      //after removing one, have to have at least one left to measure the time interval (edge case)
      if (window.size - nowIndex.getOrElse(-1) - 1 >= 2) {

        //now check the interval
        (for {
          np <- BoundedPastAndFutureWindow.nextIndex[K,V,P](nowIndex,window)  //candidate to move
          npp <- BoundedPastAndFutureWindow.nextIndex[K,V,P](Some(np),window) //new start of future TODO consider changing this.
        } yield numeric.gteq(numeric.minus(window.last._1, window(npp)._1), minFutureWidth)) //future width would be long enough after removing np
          .getOrElse(false)
      } else false

    }
    else //0 width future so easy
      nowIndex.map(ni => ni < window.size - 1).getOrElse(window.size > 0)


    if (canMove) {
      val nnow: Int = BoundedPastAndFutureWindow.nextIndex[K,V,P](nowIndex, window).get

      //Now, see if can drop some from past. Can only do this if, after dropping them, the time covered bu past() is at least minPastWidth.
      val nStartBound = numeric.minus(window(nnow)._1, minPastWidth)
      val nfirstIndex = if (numeric.gt(minPastWidth,numeric.zero)){

        //TODO speed this up with binary search
        val foundIndex = window.indexWhere(x => numeric.gt(x._1, nStartBound))
        assert(foundIndex >= 0, "could not find index where key > "+nStartBound+ " in window with keys "+
          window.map(_._1) +", nowIndex = "+nowIndex +". This should not happen!")
        // -1 as we need to keep the closest (from below) value to nStartBound
        foundIndex  - 1

      } else {
        nnow
      }

      //TODO could do below only if in strict mode, otherwise drop only in batches (efficiency ++ ?).

      val (nwindow, nnowIndex) = if (nfirstIndex > 0) {
        val newNowIndex = Math.max(nnow - nfirstIndex,0)
        assert(newNowIndex >= 0, "Problem! new nowIndex = "+newNowIndex+" is < 0: nnow = "+nnow+
          " nfirstIndex = "+nfirstIndex+", nStartBound = "+nStartBound + ", nowIndex = "+nowIndex +", window timestamps = "+window.map(_._1) +", window = "+window)
        (window.drop(nfirstIndex), Some(newNowIndex)) //rebase nowIndex as removed some
      }
      else (window, Some(nnow))

      val nPastFull = pastFull || nfirstIndex >= 0 //already full or we've just about to be able to drop (our first) one

      Some((nnowIndex, nwindow, nPastFull))

    } else None
  }



  /**
   * Attempt to move forward until now() == target time
   * returns (closest state < target, found state (== target))
   *
   * TODO improve this
   * TODO there is a more efficient way to do this... jump straight to the time then chop off the past accordingly.
   */
  def moveUntil[K: Ordering : Numeric : ClassTag, V: ClassTag, P <: Product2[K, V] : ClassTag](target: K, nowIndex: Option[Int], window: Vector[P], pastFull : Boolean,
                                                                                                       moved: (Option[Int], Vector[P], Boolean) => Option[(Option[Int], Vector[P], Boolean)]) = {

    val numeric = implicitly[Numeric[K]]

    def now(next: (Option[Int], Vector[P], Boolean)) = BoundedPastAndFutureWindow.now[K, V, P](next._1, next._2).get

    assert(nowIndex.isEmpty || numeric.lt(now((nowIndex, window,pastFull))._1, target), "something wrong with usage")

    //TODO make functional
    var prev = None: Option[(Option[Int], Vector[P], Boolean)]
    var next = moved(nowIndex, window, pastFull)
    while (!next.isEmpty && numeric.lt(now(next.get)._1, target)) {
      val n = next.get
      prev = next
      next = moved(n._1, n._2,n._3)
    }

    if (next.isDefined && now(next.get)._1 == target)
      (prev, next)
    else (prev, None) //next is after target, so gone too far, or we didn't get to target
  }

}


object ValueBoundedPastAndFutureWindowProcessors extends UsefulProcessors with Util with Serializable
{

  private def pastFullFilter[K, V, P <: Product2[K,V]](pastFull : Boolean)(w : ValueBoundedPastAndFutureWindow[K,V,P]) =
    w.pastFull || !pastFull

  /**
   * Turns a stream of values into a stream of windows.
   * @param w prototype
   * @param pastFull whether to filter out windows where past() is not "full"
   */
  def asProcess1[K : Ordering : Numeric : ClassTag, V : ClassTag, P <: Product2[K,V] : ClassTag](w : ValueBoundedPastAndFutureWindow[K,V,P], pastFull : Boolean = false): Process1[P, ValueBoundedPastAndFutureWindow[K, V, P]] = {

    val zero = w
    def op(w : ValueBoundedPastAndFutureWindow[K,V,P], r : P) = w.updated(r)

    def extract(w : ValueBoundedPastAndFutureWindow[K,V,P]) = {
      val nw = w.moved()
      (nw.getOrElse(w),nw)
    }
    accumulateAndExtractAll1(op,zero,extract).filter(pastFullFilter(pastFull) _)
  }

  private def trackStartEndZero[K] : (Option[K],Option[K]) = (None,None)

  // Maintain current [start,end) interval state
  private def trackStartEndOp[K,V](state : (Option[K],Option[K]), r : (PartitionedSeriesKey[K],V)) : (Option[K],Option[K]) = {

    //k s.t. first time seen Current
    val start = if (state._1.isEmpty && r._1.location == PartitionLocation.Current) Some(r._1.key) else state._1

    //k s.t. first time seen Future
    val end = if (state._2.isEmpty && r._1.location == PartitionLocation.Future) Some(r._1.key) else state._2

    (start,end)
  }

  // Filter s.t. now is in [start,end) interval
  private def trackStartEndFilter[K : Ordering : Numeric](state : (Option[K],Option[K]), now : Option[K]) : Boolean = {
    val num = implicitly[Numeric[K]]
    val (start,end) = state
    now.map { case k =>
      start.map(num.gteq(k,_)).getOrElse(false) &&
        end.map(num.lt(k,_)).getOrElse(true)
    }.getOrElse(false)
  }

  /**
   * Turns a stream of values within a partition into a stream of windows.
   * @param w prototype
   * @param pastFull whether to filter out windows where past() is not "full"
   */
  def fromPartitionAsProcess1[K : Ordering : Numeric : ClassTag, V : ClassTag](w : ValueBoundedPastAndFutureWindow[K,V,(K,V)], pastFull : Boolean = false):
    Process1[(PartitionedSeriesKey[K],V), ValueBoundedPastAndFutureWindow[K, V, (K,V)]] = {

    type PI = (PartitionedSeriesKey[K],V)
    type P = (K,V)
    type WR = ValueBoundedPastAndFutureWindow[K,V,P]
    type SE_STATE = (Option[K],Option[K])
    type STATE = (SE_STATE,WR)

    val zero : STATE = (trackStartEndZero[K],w)

    def op(s : STATE, r : PI) : STATE =
      (trackStartEndOp(s._1,r), s._2.updated((r._1.key,r._2)))

    def extract(s : STATE) : (STATE,Option[STATE]) = {
      val nw = s._2.moved()
      val ns = (s._1,nw.getOrElse(s._2))
      (ns,nw.map(_ => ns))
    }

    val p: Process1[PI, STATE] =
      accumulateAndExtractAll1(op,zero,extract)

    // Filter s.t. now() is in [start,end) interval, and strip start,end.
    p.filter { case (se, w) =>
      trackStartEndFilter(se, w.now().map(_._1))
    }.map( _._2).filter(pastFullFilter(pastFull) _)
  }


  /**
   * Turns a stream of grouped values (i.e. state of many I's at that SAME K)
   * into a stream of grouped windows of those values, such that now() in each of those windows is at the same K (i.e. they are in sync).
   * Note that if the maps don't always have the same keys (i.e. gaps in some of the I streams) then there will necessarily be gaps
   * in their resulting window.
   */
  def asProcess1Grouped[I,K : Ordering : Numeric : ClassTag, V : ClassTag](prototype : ValueBoundedPastAndFutureWindow[K,V,(K,V)]):
     Process1[(K,Map[I,V]), (K,Map[I,ValueBoundedPastAndFutureWindow[K,V,(K,V)]])] = {

    //type WR = ValueBoundedPastAndFutureWindow[K,V,P] //windowed record
    type WR1 = ValueBoundedPastAndFutureWindow[K,V,(K,V)]
    type TGR = (K,Map[I,V])    //'timestamped' grouped record
    type GWR = Map[I, WR1]     //grouped windowed record
    type TGWR = (K,Map[I,WR1]) //'timestamped' grouped windowed record
    type MASTER = ValueBoundedPastAndFutureWindow[K,Unit,(K,Unit)] //master (for reference) -- contains all updates
    type STATE = (MASTER,TGWR)

    val numeric = implicitly[Numeric[K]]

    val zero : STATE =
      (ValueBoundedPastAndFutureWindow(prototype.minPastWidth,prototype.minFutureWidth),
        //(numeric.negate(numeric.one),Map.empty[I,WR1]))
        (numeric.zero,Map.empty[I,WR1]))

    def op(state : STATE, r : TGR) : STATE  = {

      val (master, ws) = state

      //use timestamp of record rather than internal timestamps of R
      // NOTE: we don't know that R is timestamped, and this enforces that all the R's in the map are in sync. (*)
      val ts = r._1

      val wr: Map[I, WR1] = outerJoinAndAggregate2[I,WR1,V](ws._2, r._2, {
        case (v1: Option[WR1], v2: V) => v1.getOrElse(prototype).updated((ts,v2))
      })

      //This avoids allowing duplicated keys in master's window
      //TODO this still allows duplicated keys before the first move!
      val updatedMaster = if (master.now().map(_._1 != ts).getOrElse(true))
        master.updated((ts,()))
      else master

      (updatedMaster, (ts,wr))
    }


    def extract(state : STATE) : (STATE,Option[TGWR]) = {

      val (master, ws) = state

      master.moved() match {

        case Some(movedMaster) =>

          //Master moved. Lets get all series to catch up to it and output if so, or set to state closest to it if can't catch up yet.
          val masterTimestamp = movedMaster.now().get._1

          val mws: Map[I, (ValueBoundedPastAndFutureWindow[K, V, (K, V)], Option[ValueBoundedPastAndFutureWindow[K, V, (K, V)]])] = ws._2.mapValues{ w =>
            w.movedUntil(masterTimestamp) match {
              case (_,Some(found)) => (found,Some(found)) //found
              case (Some(closest),None) => (closest,None) //didn't find but moved
              case (None, None) => (w,None) //didn't move
            }}

          //updated state (some will have moved, others not)
          val updated: Map[I, WR1] = mws.mapValues(_._1)

          //output only those that have moved up to master
          val output = mws.flatMap{ case (i,(u,o)) => o.map((i,_))}

          val out = if(output.isEmpty)
            None //master may have moved, but none of the series have been able to keep up
          else {

            /*
            //Sanity check
            //All timestamps of updated windows must be equal to masterTimestamp
            val tss = output.map(_._2.now().get.timestamp)
            val ok = tss.foldLeft(true) {
              case (ok, t) => ok && t == masterTimestamp
            }
            assert(ok, tss)
            */

            Some((masterTimestamp,output))
          }

          //NOTE: this is the timestamp of the last record added to the window, not the time of window.now()
          val ts = ws._1

          ((movedMaster,(ts,updated)), out)

        case None => (state,None) //no change
      }
    }

    accumulateAndExtractAll1(op,zero,extract)
  }


}


