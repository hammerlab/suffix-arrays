package org.hammerlab.pageant.utils

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object Utils {
  def byteToHex(b: Byte) = {
    val s = b.toInt.toHexString.takeRight(2)
    if (s.length == 1) "0" + s else s
  }
  def b2h(byte: Byte) = byteToHex(byte)

  def byteToBinary(byte: Byte) = {
    var b = 0xff & byte
    (0 until 8).map(i => {
      val r = b % 2
      b >>= 1
      r
    }).reverse.mkString("")
  }
  def b2b(byte: Byte) = byteToBinary(byte)

  def bytesToHex(a: Array[Byte]) = a.map(byteToHex).mkString(",")
  def pt(a: Array[Byte], cols: Int = 10, binary: Boolean = true) =
    a
      .grouped(cols)
      .map(
        _.map(
          b => (if (binary) byteToBinary _ else byteToHex _)(b)
        ).mkString(" ")
      )
      .mkString("\n")

  def longToCmpFnReturn(l: Long) =
    if (l < 0) -1
    else if (l > 0) 1
    else 0

  def oneHot[T: ClassTag](t: T, zero: T, i: Int, n: Int): Array[T] = {
    var arr = ArrayBuffer[T]()
    (0 until i).foreach(_ ⇒ arr.append(zero))
    arr.append(t)
    ((i+1) until n).foreach(_ ⇒ arr.append(zero))
    arr.toArray
  }

}
