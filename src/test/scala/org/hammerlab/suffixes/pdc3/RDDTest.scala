package org.hammerlab.suffixes.pdc3

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.suffixes.base.BAMTest

trait RDDTest
  extends BAMTest {
  def sc: SparkContext
  def rdd(r: RDD[Byte]): RDD[Int]
}
