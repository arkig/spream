package spream.stream

import scalaz.stream.Process._
import scala.Some
import scalaz.{\/-, -\/, \/}
import scalaz.stream.Process.Halt
import scalaz.stream.Cause._
import scalaz.stream.Process1


trait UsefulProcessors {

  def noOp[A](a : A) = a

  def accumulateBlocks1[T,A,O](op : (A,T) => A, zero : A, complete : A => Boolean, extract : A => O): Process1[T,O] = {

    def go(acc: A): Process1[T,O] = {
      await1[T].flatMap { current =>
        val nAcc = op(acc, current)
        (if (complete(nAcc))
          emit(extract(nAcc))
        else
          halt) ++ go(nAcc)
      }
    }

    go(zero)
  }

  def accumulateBlocksWithReset1[T,A,O](op : (A,T) => A, zero : A, complete : A => Boolean, extract : A => O, reset : A => A = noOp[A] _): Process1[T,O] = {

    def go(acc: A): Process1[T,O] = {
      await1[T].flatMap { current =>
        val nAcc = op(acc, current)
        if (complete(nAcc))
          emit(extract(nAcc)) ++ go(reset(nAcc))
        else
          halt ++ go(nAcc)
      }
    }

    go(zero)
  }

  /**
   * Accumulate and extract until exhausted
   */
  def accumulateAndExtractAll1[T,A,O](op : (A,T) => A, zero : A, extract : A => (A,Option[O])): Process1[T,O] = {

    //extraction loop state
    def extractor(acc : A): Process1[T,O] = {
      extract(acc) match {
        case (nAcc,Some(o)) => emit(o) ++ extractor(nAcc)
        case (nAcc,None) => halt ++ awaiter(nAcc)
      }
    }

    def awaiter(acc: A): Process1[T,O] = {
      await1[T].flatMap { current =>
        val nAcc = op(acc, current)
        extractor(nAcc)
      }
    }

    awaiter(zero)
  }


  /**
   * Accumulate and extract at most one.
   * Actually, semantically equivalent to accumulateBlocksWithReset, just making the composition explicit (so better)
   */
  def accumulateAndExtractOne1[T,A,O](opAndExtract : (A,T) => (A,Option[O]), zero : A): Process1[T,O] = {

    def go(acc: A): Process1[T,O] = {
      await1[T].flatMap { current =>
        val (nAcc,o) = opAndExtract(acc, current)
        (o match {
          case Some(o) => emit(o)
          case None => halt
        }) ++ go(nAcc)
      }
    }

    go(zero)
  }


  def accumulateAndExtractOneWithKill1[T,A,O](opAndExtract : (A,T) => (A,Option[O],Boolean), zero : A): Process1[T,O] = {

    def go(acc: A): Process1[T,O] = {
      await1[T].flatMap { current =>
        val (nAcc,o,kill) = opAndExtract(acc, current)
        (o,kill) match {
          case (Some(o),false) => emit(o) ++ go(nAcc)
          case (None,false) => halt ++ go(nAcc)
          case (Some(o),true) => emit(o) ++ Halt(Kill)
          case (None,true) => Halt(Kill)
        }
      }
    }

    go(zero)
  }


  def filterWithKill1[T](filter : T => (Boolean,Boolean)): Process1[T,T] = {

    def go: Process1[T,T] = {
      await1[T].flatMap { current =>
        filter(current) match {
          case (true, false) => emit(current) ++ go
          case (false, false) => halt ++ go
          case (true, true) => emit(current) ++ Halt(Kill)
          case (false, true) => Halt(Kill)
        }
      }
    }
    go
  }

  def killAfter1[T](kill : T => Boolean): Process1[T,T] =
    filterWithKill1( t => (true,kill(t)))


  def filterWithFirst1[T](filter : (T,T) => Boolean) =
    accumulateAndExtractOne1[T,Option[T],T]({ (first : Option[T], i : T) =>
      val nf = (if (first.isEmpty) Some(i) else first)
      val o = if (filter(nf.get,i)) Some(i) else None
      (nf,o)
    },None)




  /**
   * TODO test
   */
  def accumulateAndExtractMany1[T,A,O](op : (A,T) => A, zero : A, extract : A => Iterable[O]): Process1[T,O] = {

    def go(acc: A): Process1[T,O] = {
      await1[T].flatMap { current =>
        val nAcc = op(acc, current)
        val os = extract(nAcc)
        (if (os.isEmpty)
          halt
        else
          os.map(emit(_)).reduce(_ ++ _)) ++ go(nAcc)
      }
    }

    go(zero)
  }

  /**
   * Extract mutates state, and the state is always the most recently generated output
   */
  def accumulateAndExtractStateMany1[T,A](op : (A,T) => A, zero : A, extract : A => Iterable[A]): Process1[T,A] = {

    def go(acc: A): Process1[T,A] = {
      await1[T].flatMap { current =>
        val nAcc = op(acc, current)
        val os = extract(nAcc)
        if (os.isEmpty)
          halt ++ go(nAcc)
        else
          os.map(emit(_)).reduce(_ ++ _) ++ go(os.last)
      }
    }

    go(zero)
  }



  def map1[T,O](f : T => O) : Process1[T,O] = {

    def go : Process1[T,O] =
      await1[T].flatMap{ next =>
        emit(f(next)) ++ go
      }

    go
  }

  def flatMapOption1[T,O](f : T => Option[O]) = {

    def go : Process1[T,O] =
      await1[T].flatMap{ next =>
        (f(next) match {
          case Some(o) => emit(o)
          case None => halt
        }) ++ go
      }

    go

  }

}

//TODO serializable only necessary here because this is used within Spark mapPartitions ...
object UsefulProcessors extends UsefulProcessors //with Serializable

