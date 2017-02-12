package org.hammerlab.suffixes.pdc3

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.magic.rdd.zip.LazyZippedWithIndexRDD._
import spire.ClassTag

/**
 * Convenience wrapper for an [[RDD]] and its zipped-index counterpart.
 */
case class RDDPair[T: ClassTag](rdd: RDD[T], zipped: RDD[(T, Long)]) {
  def sc: SparkContext = rdd.sparkContext
  def numPartitions: Int = rdd.getNumPartitions
}

object RDDPair {
  implicit def zip[T: ClassTag](rdd: RDD[T]): RDDPair[T] = new RDDPair(rdd, rdd.lazyZipWithIndex)
  implicit def apply[T: ClassTag](zipped: RDD[(T, Long)]): RDDPair[T] = new RDDPair(zipped.keys, zipped)
}
