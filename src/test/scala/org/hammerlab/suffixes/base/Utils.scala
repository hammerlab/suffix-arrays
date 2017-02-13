package org.hammerlab.suffixes.base

object Utils {
  val toI: Map[Char, Byte] = "$ACGTN".zipWithIndex.toMap.map(p => (p._1, p._2.toByte))
  val toC: Map[Byte, Char] = toI.map(_.swap)
}
