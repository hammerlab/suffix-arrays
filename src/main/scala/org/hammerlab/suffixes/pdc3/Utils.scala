package org.hammerlab.suffixes.pdc3

object Utils {
  def longToCmpFnReturn(l: Long) =
    if (l < 0) -1
    else if (l > 0) 1
    else 0
}
