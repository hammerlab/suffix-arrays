package org.hammerlab.suffixes.dc3

import java.io.{ PrintWriter, File ⇒ JFile }

import org.hammerlab.suffixes.base.BAMTest
import org.hammerlab.suffixes.base.Utils.{ toC, toI }
import org.hammerlab.test.resources.File

trait ArrayBAMTest
  extends BAMTest {

  def write(path: String,
            arr: Array[Int],
            numPerLine: Int = 10): Unit = {
    val pw = new PrintWriter(new JFile(path))
    arr.grouped(10).map(_.mkString("", "\t", "\n")).foreach(pw.write)
    pw.close()
  }

  def testBam(num: Int): Unit = {
    val name = s"bam-$num"
    test(name) {

      // Take `num` reads from the start of `1000.reads`, append a sentinel '$' to each, map to integers, and
      // distribute.
      val ts =
        scala.io.Source
          .fromFile(File("1000.reads").path)
          .getLines()
          .take(num)
          .flatMap(_ + '$')
          .map(toI(_).toInt)
          .toArray

      // Sanity-check the first 10 elements.
      ts.take(10).map(i ⇒ toC(i.toByte)).mkString("") should be("ATTTTTAAGA")

      val totalLength = 101 * num
      ts.length should be(totalLength)

      val sa = arr(ts, 6)
      sa.length should be(totalLength)

      // The first `num` elements of the suffix-array should point to the positions of the `num` sentinel values from
      // the input data, which were every 101st character (each "read" line in the input was 100 bases long).
      sa.take(num) should be(1 to num map (_ * 101 - 1) toArray)

      // Set to true to overwrite the existing "expected" file that SAs will be vetted against.
      val writeMode = false

      if (writeMode) {
        write(s"src/test/resources/$num.sa", sa)
        write(s"src/test/resources/$num.ts", ts)
      } else {
        check(num, sa, ts)
      }
    }
  }
}
