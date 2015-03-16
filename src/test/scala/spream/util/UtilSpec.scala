package spream.util

import org.specs2.Specification


class UtilSpec extends Specification with Util {
  def is = s2"""

    Util...

    Should work:
      outerJoinAndAggregate               $oja
      outerJoinAndAggregate2              $oja2

    """

  val m1 = Map(
    "a" -> 1,
    "b" -> 2,
    "c" -> 3
  )

  val m2 = Map(
    "a" -> 10,
    "d" -> 40
  )

  val mexp = Map(
    "a" -> 11,
    "b" -> 2,
    "c" -> 3,
    "d" -> 40
  )

  def oja = {
    val m = outerJoinAndAggregate[String,Int,Int,Int](m1,m2, {
      case (i1,i2) => i1.getOrElse(0) + i2.getOrElse(0)
    })
    m must_== mexp

  }

  def oja2 = {
    val m = outerJoinAndAggregate2[String,Int,Int](m1,m2, {
      case (Some(i1),i2) => i1 + i2
      case (None,i2) => i2
    })
    m must_== mexp
  }

}
