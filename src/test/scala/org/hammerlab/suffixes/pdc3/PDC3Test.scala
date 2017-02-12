package org.hammerlab.suffixes.pdc3

import org.apache.spark.rdd.RDD
import org.hammerlab.spark.test.suite.KryoSparkSuite
import org.hammerlab.suffixes.base.{ SuffixArrayBamRDDTest, SuffixArrayTest }

class PDC3Test
  extends KryoSparkSuite(classOf[Registrar])
    with SuffixArrayBamRDDTest
    with SuffixArrayTest {

  override def arr(a: Array[Int], n: Int): Array[Int] =
    PDC3(sc.parallelize(a.map(_.toLong))).map(_.toInt).collect

  override def rdd(r: RDD[Byte]): RDD[Int] =
    PDC3(r.map(_.toLong)).map(_.toInt)
}
