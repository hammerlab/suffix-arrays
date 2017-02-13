package org.hammerlab.suffixes.base

import org.hammerlab.test.resources.File
import org.scalatest.{ FunSuite, Matchers }

trait Base extends FunSuite with Matchers {
  def arr(a: Array[Int], n: Int): Array[Int]

  def intsFromFile(file: String): Array[Int] = {
    val inPath = File(file).path
    (for {
      line ← scala.io.Source.fromFile(inPath).getLines()
      if line.trim.nonEmpty
      s ← line.split("[,\t]")
      i = s.trim().toInt
    } yield
      i
    )
    .toArray
  }
}
