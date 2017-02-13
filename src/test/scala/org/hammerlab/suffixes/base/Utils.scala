package org.hammerlab.suffixes.base

/**
 * A couple utilities for converting DNA-bases between [[Char]] and [[Byte]] representations.
 */
object Utils {
  val toI: Map[Char, Byte] = "$ACGTN".zipWithIndex.toMap.map(p => (p._1, p._2.toByte))
  val toC: Map[Byte, Char] = toI.map(_.swap)
}
