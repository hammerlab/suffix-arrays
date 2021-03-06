package org.hammerlab.suffixes.pdc3

import org.hammerlab.suffixes.pdc3.Utils.longToCmpFnReturn

object JoinedCmp extends Ordering[(Long, Joined)] {

  val cmp2 = PairCmp
  val cmp3 = TripletCmp

  override def compare(x: (Long, Joined), y: (Long, Joined)): Int = {
    val (i1, j1) = x
    val (i2, j2) = y
    (i1 % 3, i2 % 3) match {
      case (0, 0) ⇒
        cmp2.compare(
          (j1.t0O.get, j1.n0O.get, i1),
          (j2.t0O.get, j2.n0O.get, i2)
        )
      case (0, 1) ⇒
        cmp2.compare(
          (j1.t0O.get, j1.n0O.get, i1),
          (j2.t0O.get, j2.n1O.get, i2)
        )
      case (1, 0) ⇒
        cmp2.compare(
          (j1.t0O.get, j1.n1O.get, i1),
          (j2.t0O.get, j2.n0O.get, i2)
        )
      case (0, 2) | (2, 0) ⇒
        cmp3.compare(
          (
            j1.t0O.get,
            j1.t1O.get,
            j1.n1O.get,
            i1
          ),
          (
            j2.t0O.get,
            j2.t1O.get,
            j2.n1O.get,
            i2
          )
        )

      // If both indices are ∈ [12]%3, then both their current indices have an name ranking their current position in
      // their `n0` fields.
      case _ ⇒
        longToCmpFnReturn(j1.n0O.get - j2.n0O.get)
    }
  }
}
