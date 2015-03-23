package spream

import org.scalatest.{PrivateMethodTester, FunSuite}
import org.apache.spark.SharedSparkContext
import org.apache.spark.rdd.RDD
import scalaz.stream.{Process1, Process, process1}
import spream.stream.{UsefulProcessors, IteratorConversions}
import spream.series.{ValueBoundedPastAndFutureWindowProcessors, ValueBoundedPastAndFutureWindow}
import scala.reflect.ClassTag


/**
 * Functions defined here to avoid serialisation issues caused by closures if they have been defined within tests.
 */
object CompContainer {

  def pastSize[K,V](w : ValueBoundedPastAndFutureWindow[K,V,(K,V)]) =
    w.past().map(_.size).getOrElse(0)

  def pastSum[K : Numeric,V](w : ValueBoundedPastAndFutureWindow[K,V,(K,V)]) =
    w.past().map(_.map(_._1).sum)

  def pastFutureSum[K : Numeric,V](w : ValueBoundedPastAndFutureWindow[K,V,(K,V)]) = {
    val num = implicitly[Numeric[K]]
    num.plus(w.past().map(_.map(_._1).sum).getOrElse(num.zero),
      w.future().map(_.map(_._1).sum).getOrElse(num.zero))
  }

  def getNowKey[K,V](w : ValueBoundedPastAndFutureWindow[K,V,(K,V)]) =
    w.now().get._1

  def meanPastSum[I,K : Numeric,V](t : K, m : Map[I,ValueBoundedPastAndFutureWindow[K,V,(K,V)]]) = {
    val num = implicitly[Numeric[K]]
    num.toDouble(m.mapValues(pastSum[K, V] _).values.flatten.sum)/m.size
  }

}

class IntegrationSuite extends FunSuite with SharedSparkContext {

  def genLinearRdd(n : Int, m : Int): RDD[(Int, Unit)] =
    sc.makeRDD(0 until n, n).flatMap { i =>
      // Lets stream into the RDD directly via a Process...
      val p = Process.range(0,m)
        .map(m*i + _)
      IteratorConversions.process0ToIterator(p)
    }.map(i => (i,())) //already sorted by construction. otherwise would do .sortBy(_._1)

  test("Distributed simple moving window operation") {

    // Compute a moving window sum.

    val (n,m,w) = (20,5,4)
    val rdd1 = genLinearRdd(n,m)

    // Distributed version
    // -------------------

    type K = PartitionedSeriesKey[Int]

    // w-1 here as last value in window is in Current.
    val rdd2: RDD[(K, Unit)] =
      MovingWindowPartitioning.movingWindowPartitioned[Int,Unit,(Int,Unit)](rdd1,w-1,0,n)

    //We know the data is 0 until n*m, and we're using the w-1 as the window width
    //for past in the partitioning, which corresponds to windowing of size w in process1.window, so can just drop
    //the partitionLocation information in this case.
    val k: Process1[(K, Unit), Int] =
      process1.lift[(PartitionedSeriesKey[Int],Unit),Int](x => x._1.key)

    //Sum, but only if full windows (window will generate partial windows at the end of  stream)
    val p = process1.window[Int](w)
      .map(v => if (v.size == w) Some(v.sum) else None)
      .filter(_.isDefined)

    val f = IteratorConversions.process1ToIterators(k |> p) _

    // Note, here p would be serialised. But that's ok as it has no context.
    val rdd3 = rdd2.mapPartitions(f)

    // Streaming version (on driver)
    // -----------------------------

    val data = rdd1.collect().map(_._1)
    val s = IteratorConversions.iteratorToProcess0(data.iterator) |> p

    assert(s.toList == rdd3.collect().toList)
  }

  /**
   * Compares a distributed streaming result to a local streaming result, where a ValueBoundedPastAndFutureWindow
   * is required for the streaming computation.
   *
   * Note that ValueBoundedPastAndFutureWindow spans a greater width than the windows in the partitioning, so buffer
   * is necessary for computations where entire past() or future() is used (rather than the part that is within the window).
   */
  def pastFutureMovingWindowTest[IK : Numeric : ClassTag, R : ClassTag]
  (rdd1 : RDD[(IK,Unit)], pastWindow : IK, futureWindow : IK, pastFull : Boolean, buffer : IK, n : Int, comp : ValueBoundedPastAndFutureWindow[IK,Unit,(IK,Unit)] => R) {

    val num = implicitly[Numeric[IK]]
    val approach = "map"
    type W = ValueBoundedPastAndFutureWindow[IK, Unit, (IK, Unit)]

    // Distributed version
    // -------------------

    type K = PartitionedSeriesKey[IK]

    // Note: buffer is used because past() in a window will also return the last value just prior to the boundary, whereas
    // the partitioning doesn't (can't) do that. So it's a work around required for certain types of window operations.
    val rdd2: RDD[(K, Unit)] =
      MovingWindowPartitioning.movingWindowPartitioned[IK,Unit,(IK,Unit)](rdd1,num.plus(pastWindow,buffer),num.plus(futureWindow,buffer),n)

    val rdd3 = rdd2.mapPartitions({

      //NOTE: this stuff gets serialized (which works, but I don't think it's necessary for Spark to implement mapPartitions that way)

      val initialState : W = ValueBoundedPastAndFutureWindow[IK,Unit,(IK,Unit)](pastWindow,futureWindow)

      val wpd: Process1[(K, Unit), W] =
        ValueBoundedPastAndFutureWindowProcessors.fromPartitionAsProcess1[IK,Unit](initialState,pastFull)

      approach match {

        case "map" =>
          val wpd2: Process1[(K, Unit), R] = wpd.map(comp)
          IteratorConversions.process1ToIterators(wpd2) _

        case "pipe" =>
          val p: Process1[W, R] = process1.lift(comp)
          val pp: Process1[(K, Unit), R] = wpd |> p
          IteratorConversions.process1ToIterators(pp) _
      }
    },true) //.map(comp) -- also possible

    val res = rdd3.collect().toList

    // Streaming version
    // -----------------

    val data: Array[(IK, Unit)] = rdd1.collect()

    val initialState : W = ValueBoundedPastAndFutureWindow[IK,Unit,(IK,Unit)](pastWindow,futureWindow)

    val wps: Process1[(IK, Unit), W] =
      ValueBoundedPastAndFutureWindowProcessors.asProcess1[IK,Unit,(IK,Unit)](initialState)

    val ps: Process1[(IK, Unit), R] = wps.map(comp)

    val s: Process[Nothing, R] = IteratorConversions.iteratorToProcess0(data.iterator) |> ps

    val res2 = s.toList

    /*println("\n\nInput:\n"+data.toList)
    println("\n\nDisrtibuted Result:\n"+res)
    println("\n\nResult:\n"+res2)
    */

    assert(res == res2)
  }

  test("Distributed past-future moving window - getNowKey") {

    val (n,m,w) = (20,5,10)
    val rdd1 = genLinearRdd(n,m).map(x => (Math.pow(x._1,1.2).toInt,x._2)) //no longer evenly distributed

    pastFutureMovingWindowTest(rdd1,w,0,false,0,n,CompContainer.getNowKey[Int,Unit] _)

  }

  test("Distributed past-future moving window - pastSize") {

    val (n,m,w,b) = (20,12,10,3) // Fails when b < 3 - expected
    val rdd1 = genLinearRdd(n,m).map(x => (Math.pow(x._1,1.2).toInt,x._2)) //no longer evenly distributed

    pastFutureMovingWindowTest(rdd1,w,0,false,b,n,CompContainer.pastSize[Int,Unit] _)

  }

  test("Distributed past-future moving window - pastSum") {

    val (n,m,w) = (20,12,13.0)
    val rdd1 = genLinearRdd(n,m).map(x => (Math.pow(x._1,1.2),x._2)) //no longer evenly distributed
    val fw = 3.0
    val b = 4.0 //Fails when e.g. b <= 3.0 - expected

    pastFutureMovingWindowTest[Double,Option[Double]](rdd1,w,fw,false,b,n,CompContainer.pastSum[Double,Unit] _)

  }

  test("Distributed past-future moving window - pastFutureSum") {

    val (n,m,pw,fw,b) = (20,12,13.0,10.0,5.0)
    val rdd1 = genLinearRdd(n,m).map(x => (Math.pow(x._1,1.2),x._2)) //no longer evenly distributed

    pastFutureMovingWindowTest[Double,Double](rdd1,pw,fw,false,b,n,CompContainer.pastFutureSum[Double,Unit] _)

  }


  /**
   * Compares a distributed streaming result to a local streaming result, where grouped ValueBoundedPastAndFutureWindows are
   * required for the streaming computation.
   */
  def groupedPastFutureMovingWindowTest[IK : Numeric : ClassTag, I : ClassTag, V : ClassTag, R : ClassTag]
    (rdd1 : RDD[(IK,Map[I,V])],pastWindow : IK, futureWindow : IK, pastFull : Boolean, buffer : IK, n : Int,
     comp : (IK,Map[I,ValueBoundedPastAndFutureWindow[IK,V,(IK,V)]]) => R) {

    val num = implicitly[Numeric[IK]]
    type M = Map[I,V]
    type W = ValueBoundedPastAndFutureWindow[IK,M, (IK, M)]
    type WO = ValueBoundedPastAndFutureWindow[IK,V, (IK, V)]
    type O = (IK,Map[I,WO])

    // Distributed version
    // -------------------

    type K = PartitionedSeriesKey[IK]

    // Note: buffer is used because past() in a window will also return the last value just prior to the boundary, whereas
    // the partitioning doesn't (can't) do that. So it's a work around required for certain types of window operations.
    val rdd2: RDD[(K, M)] =
      MovingWindowPartitioning.movingWindowPartitioned[IK,M,(IK,M)](rdd1,num.plus(pastWindow,buffer),num.plus(futureWindow,buffer),n)

    val rdd3 = rdd2.mapPartitions({

      //NOTE: this stuff gets serialized (which works, but I don't think it's necessary for Spark to implement mapPartitions that way)

      val initialState : WO = ValueBoundedPastAndFutureWindow[IK,V,(IK,V)](pastWindow,futureWindow)

      val wpd: Process1[(K, M), O] =
        ValueBoundedPastAndFutureWindowProcessors.fromPartitionedAsProcess1Grouped[I,IK,V](initialState,pastFull)

      val pd: Process1[(K, M), R] = wpd.map(Function.tupled(comp))

      IteratorConversions.process1ToIterators(pd) _

    },true)

    val res = rdd3.collect().toList

    // Streaming version
    // -----------------

    val data = rdd1.collect()

    val initialState : WO = ValueBoundedPastAndFutureWindow[IK,V,(IK,V)](pastWindow,futureWindow)

    val wps: Process1[(IK, M), O] =
      ValueBoundedPastAndFutureWindowProcessors.asProcess1Grouped[I,IK,V](initialState)

    val ps: Process1[(IK, M), R] = wps.map(Function.tupled(comp))

    val s: Process[Nothing, R] = IteratorConversions.iteratorToProcess0(data.iterator) |> ps

    val res2 = s.toList

    println("\n\nInput:\n"+data.toList)
    println("\n\nDisrtibuted Result:\n"+res)
    println("\n\nResult:\n"+res2)


    assert(res == res2)
  }


  test("Distributed past-future grouped moving window - meanPastSum") {

    val (n,m,pw,fw,b) = (20,12,13.0,10.0,5.0)
    val rdd1 = genLinearRdd(n,m).map{ case (x,_) =>
      val nx = Math.pow(x,1.2)
      val m = Map('A' -> 1.0*x, 'B' -> 2.0*x, 'C' -> 3.0*x)
      (nx,m)
    } //no longer evenly distributed

    groupedPastFutureMovingWindowTest[Double,Char,Double,Double](rdd1,pw,fw,false,b,n,CompContainer.meanPastSum[Char,Double,Double] _)

  }


}

