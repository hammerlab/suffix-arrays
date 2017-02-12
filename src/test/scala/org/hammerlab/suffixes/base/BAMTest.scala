package org.hammerlab.suffixes.base

trait BAMTest
  extends Base
    with CheckArrays {

  def check(num: Int, sa: Array[Int], ts: Array[Int]): Unit = {
    val expectedSA = intsFromFile(s"$num.sa")
    checkArrays(sa, expectedSA)

    val expectedTS = intsFromFile(s"$num.ts")
    checkArrays(ts, expectedTS)
  }

  def testBam(num: Int): Unit

  for { i ← (1 to 5) ++ Seq(10, 20, 100, 1000) } {
    testBam(i)
  }
}
