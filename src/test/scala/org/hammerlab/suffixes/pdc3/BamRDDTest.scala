package org.hammerlab.suffixes.pdc3

import org.hammerlab.suffixes.base.Utils.{ toC, toI }
import org.hammerlab.test.resources.File

trait BamRDDTest
  extends RDDTest {

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
            .textFile(File("1000.reads"), numPartitions)
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
