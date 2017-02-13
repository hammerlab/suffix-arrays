package org.hammerlab.pageant.utils

object Utils {
  def longToCmpFnReturn(l: Long) =
    if (l < 0) -1
    else if (l > 0) 1
    else 0
}
