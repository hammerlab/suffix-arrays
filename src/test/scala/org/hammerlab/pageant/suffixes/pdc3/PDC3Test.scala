package org.hammerlab.pageant.suffixes.pdc3

import org.apache.spark.rdd.RDD
import org.hammerlab.pageant.suffixes.base.{ SuffixArrayBAMTest, SuffixArrayTest }
import org.hammerlab.spark.test.suite.KryoSparkSuite
import org.scalatest.{ FunSuite, Matchers }

class PDC3Test
  extends KryoSparkSuite(classOf[PDC3Registrar])
    with SuffixArrayBAMTest
    with SuffixArrayTest {

  override def arr(a: Array[Int], n: Int): Array[Int] =
    PDC3(sc.parallelize(a.map(_.toLong))).map(_.toInt).collect

  override def rdd(r: RDD[Byte]): RDD[Int] =
    PDC3(r.map(_.toLong)).map(_.toInt)
}

class PDC3CmpFnTest extends FunSuite with Matchers {
  import PDC3.cmpFn

  test("basic 1-1 cmp") {
    cmpFn.compare(
      (1, Joined(t0O = Some(2), n0O = Some(3), n1O = Some(4))),
      (4, Joined(t0O = Some(2), n0O = Some(2), n1O = Some(1)))
    ) should be > 0
  }

  test("basic 2-0 cmp") {
    val t1 = (5L, Joined(t0O = Some(2), n0O = Some(1)))
    val t2 = (3L, Joined(t0O = Some(1), t1O = Some(2), n0O = Some(2), n1O = Some(1)))

    cmpFn.compare(t1, t2) should be > 0
    cmpFn.compare(t2, t1) should be < 0
  }
}
