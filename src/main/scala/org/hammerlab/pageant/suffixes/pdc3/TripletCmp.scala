package org.hammerlab.pageant.suffixes.pdc3

import org.hammerlab.pageant.suffixes.pdc3.PDC3.L
import org.hammerlab.pageant.utils.Utils.longToCmpFnReturn

object TripletCmp extends Ordering[(L, L, L, L)] {
  override def compare(x: (L, L, L, L), y: (L, L, L, L)): Int =
    longToCmpFnReturn(
      if (x._1 == y._1)
        if (x._1 == 0L)
          x._4 - y._4
        else if (x._2 == y._2)
          if (x._2 == 0L)
            x._4 - y._4
          else if (x._3 == y._3)
            x._4 - y._4
          else
            x._3 - y._3
        else
          x._2 - y._2
      else
        x._1 - y._1
    )
}
