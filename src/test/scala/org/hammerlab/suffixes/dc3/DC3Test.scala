package org.hammerlab.suffixes.dc3

import org.hammerlab.suffixes.base.Test

class DC3Test
  extends Test
    with ArrayBAMTest {
  override def arr(a: Array[Int], n: Int): Array[Int] = DC3.make(a, n)
}
