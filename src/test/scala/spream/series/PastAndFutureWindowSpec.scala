package spream.series


import org.specs2.Specification
import scalaz.stream.Process1
import scalaz.stream.Process._
import scala.Some
import scala.collection.immutable.IndexedSeq
import spream.{PartitionLocation, PartitionedSeriesKey}
import scala.reflect.ClassTag


// Now only used for tests
trait PastAndFutureWindowSpecHelper {

  @deprecated
  def moves[K : Ordering : Numeric : ClassTag, V : ClassTag, P <: Product2[K,V] : ClassTag](w : ValueBoundedPastAndFutureWindow[K,V,P]) = {
    doMoves[K,V,P](w.nowIndex,w.window,w.pastFull,
      ValueBoundedPastAndFutureWindow.moved[K,V,P](w.minPastWidth, w.minFutureWidth) _)
      .map{ case (i,win) => ValueBoundedPastAndFutureWindow[K,V,P](w.minPastWidth, w.minFutureWidth, i, window = win)}
  }

  @deprecated
  def doMoves[K : Ordering : Numeric : ClassTag, V : ClassTag, P <: Product2[K,V] : ClassTag](nowIndex : Option[Int], window : Vector[P], pastFull : Boolean,
                                                                                            moved : (Option[Int],Vector[P],Boolean) => Option[(Option[Int],Vector[P], Boolean)]) = {
    var next = moved(nowIndex,window, pastFull)
    var res = Seq.empty[(Option[Int],Vector[P])]
    while (!next.isEmpty) {
      val n = next.get
      res = res :+ (n._1,n._2) //TODO add _3
      next = moved(n._1,n._2,n._3)
    }
    res
  }
}


class PastAndFutureWindowSpec extends Specification with PastAndFutureWindowSpecHelper {
  def is = s2"""

    PastAndFutureWindow ...

    Should
      work step by step on a sample       $w1
      correctly provide time offsets      $o1

    """

  def tr(ts: Long) = (ts, ())

  val sample1 = Seq(
    tr(100),
    tr(200),
    tr(250),
    tr(400),
    tr(1000),
    tr(1150),
    tr(1200),
    tr(2000),
    tr(3000)
  )

  def w1 = {

    var w = ValueBoundedPastAndFutureWindow[Long, Unit, (Long, Unit)](200, 200)
    var s = sample1
    var ms = Seq.empty[ValueBoundedPastAndFutureWindow[Long, Unit, (Long, Unit)]]

    w = w.updated(s.head)
    s = s.tail
    ms = moves(w)
    assert(ms == Seq.empty)
    assert(w.now() == None)
    //println(w)

    w = w.updated(s.head)
    s = s.tail
    ms = moves(w)
    assert(ms must_== Seq.empty)
    assert(w.now() must_== None)
    //println(w)

    w = w.updated(s.head)
    s = s.tail
    ms = moves(w)
    assert(ms must_== Seq.empty)
    assert(w.now() must_== None)
    //println(w)

    w = w.updated(s.head)
    s = s.tail
    ms = moves(w)
    assert(ms must_== Seq(ValueBoundedPastAndFutureWindow[Long, Unit, (Long, Unit)](200, 200, Some(0), sample1.take(4).toVector)))
    w = ms.last
    assert(w.nowIndex must_== Some(0))
    assert(w.now() must_== Some(tr(100)))
    //println(w)

    w = w.updated(s.head)
    s = s.tail
    ms = moves(w)
    assert(ms must_== Seq(
      ValueBoundedPastAndFutureWindow[Long, Unit, (Long, Unit)](200, 200, Some(1), sample1.take(5).toVector),
      ValueBoundedPastAndFutureWindow[Long, Unit, (Long, Unit)](200, 200, Some(2), sample1.take(5).toVector)))
    w = ms.last
    assert(w.nowIndex must_== Some(2))
    assert(w.now() must_== Some(tr(250)))
    // println(w)

    //1150
    w = w.updated(s.head)
    s = s.tail
    ms = moves(w)
    assert(ms must_== Seq.empty)
    assert(w.nowIndex must_== Some(2))
    assert(w.now() must_== Some(tr(250)))
    //println(w)

    //1200
    w = w.updated(s.head)
    s = s.tail
    ms = moves(w)
    //println(ms)
    ms must_== Seq(
      ValueBoundedPastAndFutureWindow[Long, Unit, (Long, Unit)](200, 200, Some(2), sample1.take(7).tail.toVector))
    w = ms.last
    assert(w.nowIndex must_== Some(2)) //note: one dropped off back
    assert(w.now() must_== Some(tr(400)))
    //println(w)

    //2000
    //emit 2, with first one cauing drop of two.
    w = w.updated(s.head)
    s = s.tail
    ms = moves(w)
    ms must_== Seq(
      ValueBoundedPastAndFutureWindow[Long, Unit, (Long, Unit)](200, 200, Some(1), sample1.slice(3, 8).toVector), //2 dropped off
      ValueBoundedPastAndFutureWindow[Long, Unit, (Long, Unit)](200, 200, Some(2), sample1.slice(3, 8).toVector)) //no additional drops
    w = ms(0)
    assert(w.nowIndex must_== Some(1))
    assert(w.now() must_== Some(tr(1000)))
    w = ms.last
    assert(w.nowIndex must_== Some(2))
    assert(w.now() must_== Some(tr(1150)))
    //println(w)

    //3000
    //emit 1, drop 1
    w = w.updated(s.head)
    s = s.tail
    ms = moves(w)
    //println(ms)
    ms must_== Seq(
      ValueBoundedPastAndFutureWindow[Long, Unit, (Long, Unit)](200, 200, Some(2), sample1.slice(4, 9).toVector))
    w = ms.last
    assert(w.nowIndex must_== Some(2))
    assert(w.now() must_== Some(tr(1200)))
    //println(w)

    ok
  }

  def o1 = {

    val p = ValueBoundedPastAndFutureWindowProcessors.asProcess1(ValueBoundedPastAndFutureWindow[Long,String,(Long,String)](30,20))
    val data = Range(0,10).map(i => (i*10.toLong,"V"+i))
    val in = emitAll(data.toSeq).toSource
    val res = (in |> p).runLog.run
    val r = res.last

    //now and past
    assert(r.atPastOffset(0) == Some((data(6),0)))
    assert(r.atPastOffset(1) == Some((data(5),1)))
    assert(r.atPastOffset(10) == Some((data(5),1)))
    assert(r.atPastOffset(15) == Some((data(4),2)))
    assert(r.atPastOffset(30) == Some((data(3),3)))
    assert(r.atPastOffset(31) == None)

    //now and future
    assert(r.atOffset(0) == Some((data(6))))
    assert(r.atOffset(9) == Some((data(6))))
    assert(r.atOffset(10) == Some((data(7))))
    assert(r.atOffset(11) == Some((data(7))))
    assert(r.atOffset(30) == Some((data(9))))
    assert(r.atOffset(31) == None)
    assert(r.atOffset(300) == None)

    //past
    assert(r.atOffset(-1) == Some((data(5))))
    assert(r.atOffset(-31) == None)

    ok
  }

}

class PastAndFutureWindowProcessSpec extends Specification {def is = s2"""

    PastAndFutureWindow asProcess1 ...

    Should
      work (simple)                       $pw1
      work grouped with asProcess1Grouped $pgw1

      emit all values when windows are 0   $pfw0
      emit all values when future is 0     $pfw1
      correct when past and future window  $pfw2
      correct when past and future window  $pfw3
      correct when future only             $pfw4

      pastFull             $pfw5
      pastFull             $pfw6
      pastFull             $pfw7

      pastSize no future 0    $pfwpc0
      pastSize no future 1    $pfwpc1
      pastSize no future 2    $pfwpc2
      pastSize no future 3    $pfwpc3
      pastSize with future 4  $pfwpc3

      pastSum no future 0   $pfwps0
      pastSum no future 1   $pfwps1
      pastSum no future 2   $pfwps2
      pastSum no future 3   $pfwps3

    """


  def pw1 = {
    val p = ValueBoundedPastAndFutureWindowProcessors.asProcess1(ValueBoundedPastAndFutureWindow[Long,String,(Long,String)](30,20))
    val data: IndexedSeq[(Long, String)] = Range(1,12).map{ case i => ((i*10 + i%3).toLong,"V"+i)}
    val times = data.map(_._1)
    val in = emitAll(data.toSeq).toSource
    val res = (in |> p).runLog.run

    /*println("INPUT")
    println(data.mkString("\n"))
    println("OUTPUT")
    println(res.mkString("\n\n"))
    */

    res.map(_.now().get._1) must_== times.take(8)
  }




  def pgw1 = {

    type R = String

    val p: Process1[(Long, Map[String, R]), (Long, Map[String, ValueBoundedPastAndFutureWindow[Long, R, (Long, R)]])] =
      ValueBoundedPastAndFutureWindowProcessors.asProcess1Grouped[String,Long,R](ValueBoundedPastAndFutureWindow[Long,R,(Long,R)](30,20))

    val data = Range(1,12).map{ case i =>
      val m = Map("A"-> ("A"+i), "B" -> ("B"+i)) ++
        (if(i%2 == 0) Map("C"->("C"+i)) else Map.empty[String,String])
      ((i*10 + i%3).toLong,m)
    }

    val times = data.map(_._1)

    val in = emitAll(data.toSeq).toSource

    val res = (in |> p).runLog.run

    /*println("INPUT")
    println(data.mkString("\n"))
    println("OUTPUT")
    println(res.mkString("\n\n"))
    */

    assert(res.map{ r => r._2.foldLeft(true){
      case (ok,(_,v)) => ok && v.now().get._1 == r._1
    }}.reduce(_ && _), "timestamp problem -- should all be equal")

    res.map(_._1) must_== times.take(8)

  }



  def runTest[O](f : ValueBoundedPastAndFutureWindow[Int,Unit,(Int,Unit)] => O)(past : Int, future : Int, pastFull : Boolean, input : Seq[Int], out : Seq[Int]) = {
    val p = ValueBoundedPastAndFutureWindowProcessors
      .asProcess1(ValueBoundedPastAndFutureWindow[Int,Unit,(Int,Unit)](past,future),pastFull)
      .map(f)
    val data = input.map(i => (i,()))
    val in = emitAll(data.toSeq).toSource
    val res = (in |> p).runLog.run
    res must_== out
  }


  // Checks now() stream as expected
  def runNowTest = runTest(_.now().get._1) _

  def pfw0 = runNowTest(0,0,false, Range(0,10),Range(0,10))

  def pfw1 = runNowTest(5,0,false, Range(0,10),Range(0,10))

  // Note the loss of an extra output as we move away from special case (futureWidth = 0)
  // Note that future() starts at next
  def pfw2 = runNowTest(5,1,false, Range(0,10),Range(0,8))

  def pfw3 = runNowTest(5,5,false, Range(0,10),Range(0,4))

  def pfw4 = runNowTest(0,5,false, Range(0,10),Range(0,4))

  def pfw5 = runNowTest(0,4,true, Range(0,10),Range(0,5))

  def pfw6 = runNowTest(1,4,true, Range(0,10),Range(1,5))

  def pfw7 = runNowTest(1,0,true, Range(0,10),Range(1,10))


  def runPastSizeTest = runTest(_.past().map(_.size).getOrElse(0)) _

  def pfwpc0 = runPastSizeTest(2,0,true,Range(0,10),
    Range(0,8).map(_ => 3))

  def pfwpc1 = runPastSizeTest(2,0,false,Range(0,10),
    Seq(1,2) ++ Range(0,8).map(_ => 3)) //because allow non-full past

  def pfwpc2 = runPastSizeTest(1,0,true,Range(0,10),
    Range(0,9).map(_ => 2))

  def pfwpc3 = runPastSizeTest(0,0,true,Range(0,10),
    Range(0,10).map(_ => 1)) //because past() includes now()

  def pfwpc4 = runPastSizeTest(2,2,true,Range(0,10),
    Range(0,5).map(_ => 1))


  def runPastSumTest = runTest(_.past().map(_.map(_._1).sum).getOrElse(0)) _

  def pfwps0 = runPastSumTest(1,0,true,Range(0,10),
    Seq(1,3,5,7,9,11,13,15,17))

  def pfwps1 = runPastSumTest(1,0,true,Range(1,11),
    Seq(3,5,7,9,11,13,15,17,19))

  def pfwps2 = runPastSumTest(1,0,false,Range(1,11),
    Seq(1,3,5,7,9,11,13,15,17,19))

  def pfwps3 = runPastSumTest(2,0,true,Range(0,10),
    Seq(3,6,9,12,15,18,21,24))


}


class PastAndFutureWindowPartitionedProcessSpec extends Specification {
  def is = s2"""

    PastAndFutureWindow fromPartitionAsProcess1 ...

    Should
      work $ppw1

    """
  def ppw1 = {

    val p = ValueBoundedPastAndFutureWindowProcessors.fromPartitionAsProcess1(ValueBoundedPastAndFutureWindow[Int,String,(Int,String)](30,20))

    val data = Range(1,14).map{ case i =>
      val (k,v) = ((i*10 + i%3),"V"+i)
      val l = if (k < 41) PartitionLocation.Past
      else if (k > 90) PartitionLocation.Future
      else PartitionLocation.Current
      (PartitionedSeriesKey(0,k,l),v)
    }

    val in = emitAll(data.toSeq).toSource
    val keys = data.filter(_._1.location == PartitionLocation.Current).map(_._1.key)
    val res = (in |> p).runLog.run

    /*println("INPUT")
    println(data.mkString("\n"))
    println("OUTPUT")
    println(res.mkString("\n\n"))
    println("KEYS")
    println(keys.mkString(","))
    */

    res.map(_.now().get._1) must_== keys.toSeq
  }



}