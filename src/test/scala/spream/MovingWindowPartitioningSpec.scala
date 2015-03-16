package spream

import org.scalatest.FunSuite
import org.scalatest.PrivateMethodTester
import org.specs2.Specification
import spream.PartitionLocation
import MovingWindowPartitioning.IntervalSets
import PartitionLocation.EnumVal
import org.apache.spark.{SparkContext, LocalSparkContext, RangePartitioning, SharedSparkContext}

// Declare outside test class to avoid serialisation due to closure
object Container {

  def checkInsidePartition[K : Numeric, V](pastWindow : K, futureWindow : K)(it : Iterator[(PartitionedSeriesKey[K],V)]): Iterator[(Int,(K, K))] = {
    val values = it.toSeq.map(_._1)
    val numeric = implicitly[Numeric[K]]

    //Partitioning correct
    val ps = values.map(_.partition).toSet
    assert(ps.size == 1)
    val p = ps.head

    val bounds: Map[EnumVal, (K, K)] = values.groupBy(_.location).mapValues{ vs => (vs.map(_.key).min, vs.map(_.key).max)}

    //println(p+":\n"+bounds.mkString("\n"))

    (for {
      (cl, cu) <- bounds.get(PartitionLocation.Current)
    } yield {

      //Note: would prefer to have the smallest width greater than past(future)Width, but that would be very hard to implement.
      //The below ensures most efficient correct behaviour as currently defined
      //In the first partition, we may have a little more as we don't filter for this explicitly.

      bounds.get(PartitionLocation.Past).foreach{ case (pl,pu) =>
        if (p > 0)
          assert(numeric.lt(numeric.minus(pu,pl),pastWindow),
            p+": there must be less than pastWindow ("+pastWindow+") spanning items in Past, but there is "+numeric.minus(pu,pl))
        assert(numeric.gt(cl,pu), p+": upper bound of Past must be less than lower bound of Current")
      }

      bounds.get(PartitionLocation.Future).foreach{ case (fl,fu) =>
        assert(numeric.lt(numeric.minus(fu,fl),futureWindow),p+": "+ fu + ", "+cu)
        assert(numeric.gt(fl,cu))
      }

      (p,(cl,cu))
    }).iterator
  }

  def checkBetweenPartitions[K : Numeric](currentBounds : Array[(Int, (K, K))]) = {

    val numeric = implicitly[Numeric[K]]

    // Check that upperBound of current < lowerBound of next partitions current.
    currentBounds.foldLeft(Option.empty[K]){
      case (None,(p,(l,u))) => Some(u)
      case (Some(pu),(p,(l,u))) =>
        assert(numeric.lt(pu,l), "Current buckets must not overlap and must be in order")
        Some(u)
    }

  }

}

class MovingWindowPartitioningSuite extends FunSuite with SharedSparkContext with PrivateMethodTester {

  test("MovingWindowPartitioning manual and random data") {

    val rdd = sc.makeRDD(0 until 20, 20).flatMap { i =>
      val random = new java.util.Random(i)
      Iterator.fill(i)(random.nextDouble())
    }.map(i => (i,i.toString)).sortBy(_._1).cache()

    val rb = RangePartitioning.rangeBounds(rdd,10)
    //println(rb.mkString(",")+"\n")

    val is: IntervalSets[Double] = MovingWindowPartitioning.intervalSets(rb,0.05,0.05)
    //println(is+"\n")

    val rdd1 = MovingWindowPartitioning.duplicateToIntervals[Double,String,(Double,String)](rdd,is)
    //println(rdd1.collect().mkString("\n"))

    val rdd2 = MovingWindowPartitioning.applyPartitioning[Double,String,Product2[PartitionedSeriesKey[Double],String]](rdd1,rb)
    //println(rdd2.collect().mkString("\n"))

    val currentBounds: Array[(Int, (Double, Double))] = rdd2
      .mapPartitions(Container.checkInsidePartition[Double,String](0.05,0.05) _,true)
      .collect().sortBy(_._1)
    //println(currentBounds.mkString("\n")+"\n")

    Container.checkBetweenPartitions(currentBounds)
  }

  def rdd1(partitions : Int) =
    sc.makeRDD(0 until 20, partitions)
      .flatMap { i => (i until 2*i+10)}
      .map(i => (i,())).sortBy(_._1).cache()

  test("MovingWindowPartitioning.movingWindowPartitioned fixed data") {
    val rdd = rdd1(20)
    val rdd2 = MovingWindowPartitioning.movingWindowPartitioned[Int,Unit,(Int,Unit)](rdd,10,5,9)
    val currentBounds = rdd2
      .mapPartitions(Container.checkInsidePartition[Int,Unit](10,5) _,true)
      .collect().sortBy(_._1)
    Container.checkBetweenPartitions(currentBounds)
  }

  test("MovingWindowPartitioning.movingWindowPartitioned fixed data, expand partitions") {
    val rdd = rdd1(10)
    val rdd2 = MovingWindowPartitioning.movingWindowPartitioned[Int,Unit,(Int,Unit)](rdd,10,10,21)
    val currentBounds = rdd2
      .mapPartitions(Container.checkInsidePartition[Int,Unit](10,10) _,true)
      .collect().sortBy(_._1)
    Container.checkBetweenPartitions(currentBounds)
  }

  test("MovingWindowPartitioning.movingWindowPartitioned fixed data, one partition") {
    val rdd = rdd1(10)
    val rdd2 = MovingWindowPartitioning.movingWindowPartitioned[Int,Unit,(Int,Unit)](rdd,10,10,1)
    val currentBounds = rdd2
      .mapPartitions(Container.checkInsidePartition[Int,Unit](10,10) _,true)
      .collect().sortBy(_._1)
    Container.checkBetweenPartitions(currentBounds)
  }

  test("MovingWindowPartitioning.movingWindowPartitioned fixed data, one partition, no past or future") {
    val rdd = rdd1(10)
    val rdd2 = MovingWindowPartitioning.movingWindowPartitioned[Int,Unit,(Int,Unit)](rdd,0,0,1)
    val currentBounds = rdd2
      .mapPartitions(Container.checkInsidePartition[Int,Unit](0,0) _,true)
      .collect().sortBy(_._1)
    Container.checkBetweenPartitions(currentBounds)
  }

}

class DuplicateToIntervalsIteratorSpec extends Specification {
  def is = s2"""

    DuplicateToIntervalsIterator...

    should work:
      empty values             $t1
      single value             $t2
      single value             $t3
      few values               $t4
    """

  val partitions = Array(10,20)
  val intervalSets = MovingWindowPartitioning.intervalSets(partitions, 5,5)

  val C = PartitionLocation.Current
  val P = PartitionLocation.Past
  val F = PartitionLocation.Future

  println(intervalSets.mkString("\n"))

  def evaluate(it : Iterator[(Int,Unit)]) : List[Product2[PartitionedSeriesKey[Int], Unit]] =
    new DuplicateToIntervalsIterator[Int,Unit,(Int,Unit)](it,intervalSets).toList

  def gen[K](l : List[K]) = l.map(i => (i,()))

  def t1 = evaluate(gen(Nil).iterator).must_==(Nil)

  def t2 = {
    val values = gen(List(10))
    val r = evaluate(values.iterator).map( p => (p._1.partition, p._1.location))
    r.must_==(List((0,F), (1,C)))
  }

  def t3 = evaluate(gen(15 :: Nil).iterator) must_== (gen(List(
      PartitionedSeriesKey(1,15,C),
      PartitionedSeriesKey(2,15,P)
    )))

  def t4 = {
    val values = gen(List(10,15,20))
    val r = evaluate(values.iterator)
    //println(r.mkString("\n"))
    r.must_==(gen(List(
      PartitionedSeriesKey(0,10,F),
      PartitionedSeriesKey(1,10,C),
      PartitionedSeriesKey(1,15,C),
      PartitionedSeriesKey(2,15,P),
      PartitionedSeriesKey(1,20,F),
      PartitionedSeriesKey(2,20,C)
    )))
  }

}


class MovingWindowPartitioningSpec extends Specification { def is = s2"""

    Timeseries windowing partitioning...

    intervalSets should work:
      without overlap, bounds distinct          $t1
      0 windows                                 $t2
      without overlap, with equal bounds        $t3
      with overlap, with equal bounds           $t4
    """

  val partitions1 = Array(10,30,45)

  val C = PartitionLocation.Current
  val P = PartitionLocation.Past
  val F = PartitionLocation.Future


  def t1 = {
    val is = MovingWindowPartitioning.intervalSets(partitions1, 5,5)
    println(is)
    is must_== List(
      (Some(5),Set((0,C))),
      (Some(10),Set((0,C), (1,P))),
      (Some(15),Set((0,F), (1,C))),
      (Some(25),Set((1,C))),
      (Some(30),Set((1,C), (2,P))),
      (Some(35),Set((1,F), (2,C))),
      (Some(40),Set((2,C))),
      (Some(45),Set((2,C), (3,P))),
      (Some(50),Set((2,F), (3,C))),
      (None,Set((3,C)))
    )
  }

  def t2 = {
    val is = MovingWindowPartitioning.intervalSets(partitions1, 0,0)
    //println(is)
    is must_== List(
      (Some(10),Set((0,C))),
      (Some(30),Set((1,C))),
      (Some(45),Set((2,C))),
      (None,Set((3,C)))
    )
  }

  def t3 = {
    val is = MovingWindowPartitioning.intervalSets(partitions1, 15,15)
    //println(is)
    is must_== List(
      (Some(-5),Set((0,C))),
      (Some(10),Set((0,C), (1,P))),
      (Some(15),Set((0,F), (1,C))),
      (Some(25),Set((0,F), (1,C), (2,P))),
      (Some(30),Set((1,C), (2,P))),
      (Some(45),Set((1,F),(2,C), (3,P))),
      (Some(60),Set((2,F), (3,C))),
      (None,Set((3,C)))
    )
  }

  def t4 = {
    val is = MovingWindowPartitioning.intervalSets(partitions1, 20,20)
    //println(is)
    is must_== List(
      (Some(-10),Set((0,C))),
      (Some(10),Set((0,C), (1,P))),
      (Some(25),Set((0,F), (1,C), (2,P))),
      (Some(30),Set((0,F), (1,C), (2,P), (3,P))),
      (Some(45),Set((1,F),(2,C), (3,P))),
      (Some(50),Set((1,F),(2,F), (3,C))),
      (Some(65),Set((2,F), (3,C))),
      (None,Set((3,C)))
    )
  }

}
