package org.hammerlab.suffixes.base

import java.io.{ PrintWriter, File ⇒ JFile }

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.suffixes.base.Utils.{ toC, toI }
import org.hammerlab.test.resources.File
import org.scalatest.{ FunSuite, Matchers }

trait SuffixArrayTestBase extends FunSuite with Matchers {
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

trait SuffixArrayRDDTest
  extends SuffixArrayTestBase {
  def sc: SparkContext
  def rdd(r: RDD[Byte]): RDD[Int]
}

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

trait SuffixArrayBAMTest
  extends SuffixArrayTestBase
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

trait SuffixArrayArrayBAMTest
  extends SuffixArrayBAMTest {

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

trait SuffixArrayBamRDDTest
  extends SuffixArrayBAMTest
    with SuffixArrayRDDTest {

  def testBam(num: Int): Unit = {

    def numPartitions =
      if (num < 10)
        4
      else
        10

    val name = s"bam-$num-$numPartitions"
    test(name) {

      // Take `num` reads from the start of `1000.reads`, append a sentinel '$' to each, map to integers, and
      // distribute.
      val ts =
        sc.parallelize(
          sc
            .textFile(File("1000.reads").path, numPartitions)
            .take(num)
            .flatMap(_ + '$')
            .map(toI)
          ,
          numPartitions
        )

      ts.getNumPartitions should be(numPartitions)

      // Sanity-check the first 10 elements.
      ts.take(10).map(toC).mkString("") should be("ATTTTTAAGA")

      val totalLength = 101 * num
      ts.count should be(totalLength)

      val sa = rdd(ts)
      sa.count should be(totalLength)

      // The first `num` elements of the suffix-array should point to the positions of the `num` sentinel values from
      // the input data, which were every 101st character (each "read" line in the input was 100 bases long).
      sa.take(num) should be(1 to num map (_ * 101 - 1) toArray)

      sa.getNumPartitions should be(numPartitions)

      check(num, sa.collect, ts.collect.map(_.toInt))
    }
  }
}

trait SuffixArrayDNATest extends SuffixArrayTestBase {
  test("SA-1") {
    arr(Array(0, 1, 2, 0, 1, 1), 4) should be(Array(0, 3, 5, 4, 1, 2))
  }

  test("SA-2") {
    // Inserting elements at the end of the above array.
    arr(Array(0, 1, 2, 0, 1, 1, 0), 4) should be(Array(0, 3, 6, 5, 4, 1, 2))
    arr(Array(0, 1, 2, 0, 1, 1, 1), 4) should be(Array(0, 3, 6, 5, 4, 1, 2))
    arr(Array(0, 1, 2, 0, 1, 1, 2), 4) should be(Array(0, 3, 4, 1, 5, 2, 6))
    arr(Array(0, 1, 2, 0, 1, 1, 3), 4) should be(Array(0, 3, 4, 1, 5, 2, 6))
  }

  test("SA-3") {
    // Inserting elements at index 3 in the last array above.
    arr(Array(0, 1, 2, 0, 0, 1, 1, 3), 4) should be(Array(0, 3, 4, 5, 1, 6, 2, 7))
    arr(Array(0, 1, 2, 1, 0, 1, 1, 3), 4) should be(Array(0, 4, 3, 5, 1, 6, 2, 7))
    arr(Array(0, 1, 2, 2, 0, 1, 1, 3), 4) should be(Array(0, 4, 5, 1, 6, 3, 2, 7))
    arr(Array(0, 1, 2, 3, 0, 1, 1, 3), 4) should be(Array(0, 4, 5, 1, 6, 2, 3, 7))
  }

  test(s"SA-4") {
    // Inserting elements at index 5 in the first array in the second block above.
    arr(Array(0, 1, 2, 0, 1, 0, 1, 0), 4) should be(Array(0, 3, 5, 7, 4, 6, 1, 2))
    arr(Array(0, 1, 2, 0, 1, 1, 1, 0), 4) should be(Array(0, 3, 7, 6, 5, 4, 1, 2))
    arr(Array(0, 1, 2, 0, 1, 2, 1, 0), 4) should be(Array(0, 3, 7, 6, 1, 4, 2, 5))
    arr(Array(0, 1, 2, 0, 1, 3, 1, 0), 4) should be(Array(0, 3, 7, 6, 1, 4, 2, 5))
  }

  test("SA-5: zeroes") {
    for { i <- 0 to 16 } {
      withClue(s"$i zeroes:") {
        arr(Array.fill(i+1)(0), 4) should be((0 to i).toArray)
      }
    }
  }
}

trait SuffixArrayOtherTest extends SuffixArrayTestBase {
  test(s"SA-6") {
    arr(Array(5, 1, 3, 0, 4, 5, 2), 7) should be(Array(3, 1, 6, 2, 4, 0, 5))
    arr(Array(2, 2, 2, 2, 0, 2, 2, 2, 1), 9) should be(Array(4, 8, 3, 7, 2, 6, 1, 5, 0))
  }

  test(s"random 100") {
    val a = Array(
      2, 7, 8, 7, 5, 5, 2, 3, 8, 1,  // 0
      2, 9, 2, 2, 6, 2, 9, 9, 6, 7,  // 1
      1, 8, 5, 1, 1, 7, 8, 7, 4, 6,  // 2
      8, 1, 5, 1, 6, 3, 9, 3, 7, 8,  // 3
      4, 1, 3, 7, 9, 8, 2, 4, 8, 1,  // 4
      5, 8, 1, 1, 6, 7, 2, 1, 8, 2,  // 5
      4, 9, 9, 2, 5, 6, 8, 2, 6, 8,  // 6
      7, 8, 1, 8, 1, 3, 3, 7, 7, 5,  // 7
      6, 1, 1, 2, 3, 3, 7, 3, 1, 9,  // 8
      8, 8, 8, 6, 9, 5, 5, 9, 4, 8   // 9
    )

    val expected =
      Array(
        81, 52, 23, 82, 9, 74, 41, 31, 49, 33, 53, 24, 72, 57, 20, 88,         // 1's
        56, 12, 83, 6, 46, 59, 63, 13, 67, 0, 10, 15,                          // 2's
        87, 84, 75, 85, 76, 37, 42, 7, 35,                                     // 3's
        40, 28, 98, 47, 60,                                                    // 4's
        22, 32, 5, 4, 95, 79, 64, 50, 96,                                      // 5's
        80, 14, 34, 18, 54, 29, 65, 68, 93,                                    // 6's
        19, 55, 86, 27, 3, 78, 77, 70, 38, 25, 1, 43,                          // 7's
        99, 51, 8, 73, 30, 48, 71, 45, 58, 66, 39, 21, 92, 26, 2, 69, 91, 90,  // 8's
        11, 62, 36, 97, 94, 17, 44, 89, 61, 16                                 // 9's
      )

    arr(a, 10) should be (expected)
  }

  test(s"random 1000") {
    val a = intsFromFile("random1000.in")
    val expected = intsFromFile("random1000.expected")
    arr(a, 10) should be(expected)
  }
}

trait SuffixArrayTest
  extends SuffixArrayOtherTest
    with SuffixArrayDNATest
