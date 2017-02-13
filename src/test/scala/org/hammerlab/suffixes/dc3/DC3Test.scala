package org.hammerlab.suffixes.dc3

import org.hammerlab.suffixes.base.SuffixArrayTest

class DC3Test extends SuffixArrayTest {
  override def arr(a: Array[Int], n: Int): Array[Int] = DC3.make(a, n)
}
