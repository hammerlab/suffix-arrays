package org.hammerlab.suffixes.base

import org.scalatest.Matchers

trait CheckArrays {
  self: Matchers ⇒
  def checkArrays(actual: Array[Int], expected: Array[Int]): Unit = {
    actual.length should be(expected.length)
    for {
      idx ← actual.indices
      actualElem = actual(idx)
      expectedElem = expected(idx)
    } {
      withClue(
        ", idx %d: %s vs. %s"
        .format(
          idx,
          actual.slice(idx - 5, idx + 5).mkString(","),
          expected.slice(idx - 5, idx + 5).mkString(",")
        )
      ) {
        actualElem should be(expectedElem)
      }
    }
  }
}
