package org.hammerlab.pageant.suffixes.pdc3

import org.hammerlab.pageant.suffixes.pdc3.PDC3.L
import org.hammerlab.pageant.utils.PageantSuite
import org.hammerlab.spark.test.suite.SparkSerialization

class JoinedSerializerTest
  extends PageantSuite
    with SparkSerialization {

  def makeJ(t0: L = -1, t1: L = -1, n0: L = -1, n1: L = -1): Joined = {
    def opt(l: Long) = if (l < 0) None else Some(l)
    Joined(opt(t0), opt(t1), opt(n0), opt(n1))
  }

  def testFn(name: String, size: Int, t0: Int = -1, t1: Int = -1, n0: Int = -1, n1: Int = -1): Unit = {
    test(name) {
      val joined = makeJ(t0, t1, n0, n1)
      val bytes = serialize(joined)
      bytes.length should be(size)

      val j = deserialize[Joined](bytes)

      j should be(joined)
    }
  }

  testFn("t0-t1-n0-n1", 32,  1,  2,  3,  4)

  testFn("t0-t1-n0",    24,  1,  2,  3, -1)
  testFn("t0-t1-n1",    24,  1,  2, -1,  4)
  testFn("t0-n0-n1",    24,  1, -1,  3,  4)
  testFn("t1-n0-n1",    24, -1,  2,  3,  4)

  testFn("t0-t1",       16,  1,  2, -1, -1)
  testFn("t0-n0",       16,  1, -1,  3, -1)
  testFn("t0-n1",       16,  1, -1, -1,  4)
  testFn("t1-n0",       16, -1,  2,  3, -1)
  testFn("t1-n1",       16, -1,  2, -1,  4)
  testFn("n0-n1",       16, -1, -1,  3,  4)

  testFn("t0",           8,  1, -1, -1, -1)
  testFn("t1",           8, -1,  2, -1, -1)
  testFn("n0",           8, -1, -1,  3, -1)
  testFn("n1",           8, -1, -1, -1,  4)

  testFn("none",         8, -1, -1, -1, -1)
}
