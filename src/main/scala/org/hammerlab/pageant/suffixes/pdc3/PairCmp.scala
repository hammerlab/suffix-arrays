package org.hammerlab.pageant.suffixes.pdc3

import org.hammerlab.pageant.suffixes.pdc3.PDC3.L
import org.hammerlab.pageant.utils.Utils.longToCmpFnReturn

object PairCmp extends Ordering[(L, L, L)] {
  override def compare(x: (L, L, L), y: (L, L, L)): Int =
    longToCmpFnReturn(
      if (x._1 == y._1)
        if (x._1 == 0L)
          x._3 - y._3
        else
          x._2 - y._2
      else
        x._1 - y._1
    )
}
