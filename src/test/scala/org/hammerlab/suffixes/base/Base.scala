package org.hammerlab.suffixes.base

import org.hammerlab.test.resources.File
import org.scalatest.{ FunSuite, Matchers }

trait Base extends FunSuite with Matchers {
  def arr(a: Array[Int], n: Int): Array[Int]

  def intsFromFile(file: String): Array[Int] = {
    (for {
      line ← scala.io.Source.fromFile(File(file)).getLines()
      if line.trim.nonEmpty
      s ← line.split("[,\t]")
      i = s.trim().toInt
    } yield
      i
    )
    .toArray
  }
}
