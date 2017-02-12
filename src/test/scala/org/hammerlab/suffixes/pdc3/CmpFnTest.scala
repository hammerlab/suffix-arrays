package org.hammerlab.suffixes.pdc3

import org.scalatest.{ FunSuite, Matchers }

class CmpFnTest extends FunSuite with Matchers {
  import PDC3.cmpFn

  test("basic 1-1 cmp") {
    cmpFn.compare(
      (1, Joined(t0O = Some(2), n0O = Some(3), n1O = Some(4))),
      (4, Joined(t0O = Some(2), n0O = Some(2), n1O = Some(1)))
    ) should be > 0
  }

  test("basic 2-0 cmp") {
    val t1 = (5L, Joined(t0O = Some(2), n0O = Some(1)))
    val t2 = (3L, Joined(t0O = Some(1), t1O = Some(2), n0O = Some(2), n1O = Some(1)))

    cmpFn.compare(t1, t2) should be > 0
    cmpFn.compare(t2, t1) should be < 0
  }
}
