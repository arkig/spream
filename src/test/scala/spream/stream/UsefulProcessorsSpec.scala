package spream.stream

import org.specs2.Specification
import scalaz.stream.Process._
import scala.Some
import scalaz.stream.Process
import scalaz.concurrent.Task

class UsefulProcessorsSpec extends Specification with UsefulProcessors { def is = s2"""

    Process examples...

    Should work:
      accumulateBlocks1   $e4
      accumulateBlocksWithReset1   $e5
      map1                   $m1
      accumulateAndExtractAll1   $aea
      failAfter1  $fa1

    """


  def e4 = {
    val p1 = emitAll((1 to 10).toSeq).toSource
    val res = (p1 |> accumulateBlocks1((a : Int,b : Int) => a+b,0, (x : Int) => (x%2 != 0), (x : Int) => x)).runLog.run
    //println(res)
    res.must_==(Vector(1, 3, 15, 21, 45, 55))
  }

  def e5 = {
    val p1 = emitAll((1 to 10).toSeq).toSource
    val res = (p1 |> accumulateBlocksWithReset1((a : Int,b : Int) => a+b,0, (x : Int) => (x%2 != 0), (x : Int) => x)).runLog.run
    //println(res)
    res.must_==(Vector(1, 3, 15, 21, 45, 55))
  }


  def m1 = {
    val p1 = emitAll((1 to 5).toSeq).toSource
    val p2 = map1[Int,String]{case i => (i*2).toString}
    val p3 = map1[String,String]{case i => i + "s"}
    //val p23 = p2 ++ p2 //i believe this would run p3 after the end of stream of p2, so won't work
    //val p23 = p2 |> p2 //can't pipe as no context to execute, hence makes no sense for |> to be defined for process1
    //val p23 = p2.flatMap(p3) //no, because flatmap takes input => process1
    val p23a = p2.flatMap(x => emit(x+"s")) // Ok
    val p23b = p2.map(x => x+"s") // Ok
    val go: Process[Task, String] = p1 |> p2 |> p3
    val res = go.runLog.run
    val resa = (p1 |> p23a).runLog.run
    val resb = (p1 |> p23b).runLog.run
    //println(res)
    res must_== Vector("2s","4s","6s","8s","10s")
    res must_== resa
    res must_== resb
  }

  def aea = {

    //If the cumulative sum is even, output it n times, where n is the number of ints seen since the last output

    val zero = (0,0)

    def op(acc : (Int,Int), n : Int) = (acc._1 + n, acc._2 + 1)

    def extract(acc : (Int,Int)) =
      if (acc._1 % 2 == 0 && acc._2 > 0)
        (acc.copy(_2 = acc._2 - 1), Some(acc._1))
      else
        (acc, None)

    val p = accumulateAndExtractAll1(op,zero,extract)

    val in = emitAll((1 to 17).toSeq).toSource
    val res = (in |> p).runLog.run

    println(res)
    res must_== Vector(6,6,6,10,28,28,28,36,66,66,66,78,120,120,120,136)
  }


  def fa1 = {

    val in = emitAll((1 to 20).toSeq).toSource

    val p = filterWithKill1[Int]{ case i: Int =>
      if (i < 10)
        (true,false)
      else if (i == 10)
        (true,true)
      else
        sys.error("should never get here")
    }


    val res = (in |> p).runLog.run

    println(res)
    res must_== (1 to 10).toVector

  }

}



