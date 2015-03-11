package spream

import org.scalatest.{PrivateMethodTester, FunSuite}
import org.apache.spark.SharedSparkContext
import org.apache.spark.rdd.RDD
import scalaz.stream.{Process1, Process, process1}
import spream.stream.IteratorConversions
import spream.series.ValueBoundedPastAndFutureWindow


class IntegrationSuite extends FunSuite with SharedSparkContext {

  //
  def genLinearRdd(n : Int, m : Int, w : Int) =
    sc.makeRDD(0 until n, n).flatMap { i =>
      // Lets stream into the RDD directly via a Process...
      val p = Process.range(0,m)
        .map(m*i + _)
      IteratorConversions.process0ToIterator(p)
    }.map(i => (i,())) //already sorted by construction. otherwise would do .sortBy(_._1)

  test("Distributed moving window operation") {

    // Compute a moving window sum.

    // Distributed version
    // -------------------

    val (n,m,w) = (20,5,4)
    val rdd1 = genLinearRdd(n,m,w)

    // w-1 here as last value in window is in Current.
    val rdd2: RDD[(PartitionedSeriesKey[Int], Unit)] =
      MovingWindowPartitioning.movingWindowPartitioned[Int,Unit,(Int,Unit)](rdd1,w-1,0,n)

    //We know the data is 0 until n*m, and we're using the w-1 as the window width
    //for past in the partitioning, which corresponds to windowing of size w in process1.window, so can just drop
    //the partitionLocation information in this case.
    val k = process1.lift[(PartitionedSeriesKey[Int],Unit),Int](x => x._1.key)

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

  /*
  test("Moving window using partition location") {

    val (n,m,w) = (20,5,10)
    val rdd1 = genLinearRdd(n,m,w).map(x => (Math.pow(x._1,1.2),x._2)) //no longer evenly distributed

    type K = PartitionedSeriesKey[Double]

    val rdd2: RDD[(K, Unit)] =
      MovingWindowPartitioning.movingWindowPartitioned[Double,Unit,(Double,Unit)](rdd1,w,0,n)

    val pfw: Process1[(K, Unit), ValueBoundedPastAndFutureWindow[K, Unit, (K, Unit)]] =
      ValueBoundedPastAndFutureWindow.asProcess1[K,Unit,(K,Unit)](ValueBoundedPastAndFutureWindow[K,Unit,(K,Unit)](w,0.0))

    val p = pfw.filter(_.)

    val f = IteratorConversions.process1ToIterators(pfw) _

    rdd2.mapPartitions()


  }
*/
}

