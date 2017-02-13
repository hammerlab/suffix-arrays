package org.hammerlab.suffixes.pdc3

import org.hammerlab.suffixes.pdc3.PDC3.Name
import Utils.longToCmpFnReturn

object JoinedCmp extends Ordering[(Name, Joined)] {

  val cmp2 = PairCmp
  val cmp3 = TripletCmp

  override def compare(x: (Name, Joined), y: (Name, Joined)): Int = {
    val (i1, j1) = x
    val (i2, j2) = y
    (i1 % 3, i2 % 3) match {
      case (0, 0) =>
        cmp2.compare(
          (j1.t0O.get, j1.n0O.getOrElse(0L), i1),
          (j2.t0O.get, j2.n0O.getOrElse(0L), i2)
        )
      case (0, 1) =>
        cmp2.compare(
          (j1.t0O.get, j1.n0O.getOrElse(0L), i1),
          (j2.t0O.get, j2.n1O.getOrElse(0L), i2)
        )
      case (1, 0) =>
        cmp2.compare(
          (j1.t0O.get, j1.n1O.getOrElse(0L), i1),
          (j2.t0O.get, j2.n0O.getOrElse(0L), i2)
        )
      case (0, 2) | (2, 0) =>
        cmp3.compare(
          (j1.t0O.get, j1.t1O.getOrElse(0L), j1.n1O.getOrElse(0L), i1),
          (j2.t0O.get, j2.t1O.getOrElse(0L), j2.n1O.getOrElse(0L), i2)
        )
      case _ =>
        longToCmpFnReturn(j1.n0O.get - j2.n0O.get)
    }
  }
}
