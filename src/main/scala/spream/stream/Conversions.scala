package spream.stream

import scalaz.stream._
import scalaz.stream.Process._
import scalaz.stream.Cause.{Kill, End, Terminated, Error}
import scalaz.concurrent.Task
import scalaz.{-\/, \/-}


/**
 * Driving a non-context scalaz-stream Process externally and expose its
 * stream of values as an iterator.
 */
class Process0Iterator[O](p : Process[Nothing,O]) extends Iterator[O] {

  type P = Process[Nothing,O]
  val queue = scala.collection.mutable.Queue[O]()
  var current = p
  var hasNextCalled = false //Defensive :/ let's ensure correct usage

  private def nextEmitted(p : P) = {

    def go(cur: P): (Seq[O],P) = {
      cur.step match {
        case s: Step[Nothing,O]@unchecked =>
          (s.head, s.next) match {
            case (Emit(os), cont) =>
              (os, cont.continue.asInstanceOf[P])
            case (awt:Await[Nothing,Any,O]@unchecked, cont) =>
              go(cont.continue.asInstanceOf[P])
          }
        case Halt(x) => (Seq.empty, Halt(x).asInstanceOf[P])
      }
    }

    go(p)
  }

  override def hasNext: Boolean = {
    if (queue.isEmpty) {
      current match {
        case Halt(Error(rsn)) =>
          throw rsn
        case default =>
          val (os,next) = nextEmitted(current)
          queue ++= os
          current = next
      }
    }
    hasNextCalled = true
    !queue.isEmpty
  }

  override def next() = {
    assert(hasNextCalled,"Must call hasNext() at least once before each next() for this to work.")
    hasNextCalled = false
    queue.dequeue()
  }
}

/**
 * Driving a Process[Task,O] externally and expose its
 * stream of values as an iterator, using the toTask trick.
 * NOTE: must completely consume the iterator otherwise resources will not be closed.
 * TODO current versions of scalaz.stream have removed toTask from the API.
 * TODO switch to a more principled approach once this is provided by scalaz.stream
 */
class TaskProcessIterator[O](p : Process[Task,O]) extends Iterator[O] {

  val task : Task[O] = p.toTask
  val queue = scala.collection.mutable.Queue[O]()
  var hasNextCalled = false //Defensive :/ let's ensure correct usage

  private def step() = {
    task.attemptRun match {
      case \/-(o) =>
        queue += o
      case -\/(Terminated(End)) =>
        // Normal termination
      case -\/(Terminated(Kill)) =>
        // Requested termination
      case -\/(Terminated(Error(rsn))) =>
        throw rsn;
    }
  }

  override def hasNext: Boolean = {
    if (queue.isEmpty) step()
    hasNextCalled = true
    !queue.isEmpty
  }

  override def next() = {
    assert(hasNextCalled,"Must call hasNext() at least once before each next() for this to work.")
    hasNextCalled = false
    queue.dequeue()
  }

}


trait IteratorConversions {

  def process0ToIterator[O](p : Process[Nothing,O]) = new Process0Iterator(p)

  def iteratorToProcess0[A](it : Iterator[A]) : Process[Nothing,A] = {

    def next : Process[Nothing,A] =
      go(if (it.hasNext) Some(it.next()) else None)

    def go(nextValue : Option[A]): Process[Nothing,A] =
      nextValue.map(v => emit(v) ++ next).getOrElse(halt)

    next
  }

  def taskProcessToIterator[O](p : Process[Task,O]) = new TaskProcessIterator(p)

  def process1ToIterators[I,O](p : Process1[I,O])(it : Iterator[I]) : Iterator[O] =
    new Process0Iterator(iteratorToProcess0(it) |> p)

  def iteratorsToProcess1[I,O](f : Iterator[I] => Iterator[O]) = {
    //TODO how? (just for fun... ;-))
    ???
  }

}

object IteratorConversions extends IteratorConversions