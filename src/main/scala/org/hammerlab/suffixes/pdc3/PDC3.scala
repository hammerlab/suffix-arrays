package org.hammerlab.suffixes.pdc3

import java.io.{ ObjectInputStream, ObjectOutputStream }

import com.esotericsoftware.kryo.Kryo
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoRegistrator
import org.hammerlab.magic.rdd.sort.SortRDD._
import org.hammerlab.magic.rdd.serde.SequenceFileSerializableRDD._
import org.hammerlab.magic.rdd.sliding.SlidingRDD._
import org.hammerlab.suffixes.dc3.DC3
import org.hammerlab.suffixes.pdc3.PDC3.Name

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

// done:
// - fix 0-padding subtleties
// - less caching

// probably done:
// - profile GCs

// TODO:
// - PDC7

// not possible?
// - return indexed SA, save ZWI() job

//case class NameTuple



object PDC3 {

  type L = Long

  type PartitionIdx = Int
  type Name = L
  type L3 = (L, L, L)
  type L3I = (L3, L)
  type NameTuple = (PartitionIdx, Name, L, L3, L3)
  type OL = Option[L]

  def apply(t: RDD[L]): RDD[L] = apply(t, t.count)

  def apply(t: RDD[L],
            count: Long,
            checkpointConfig: CheckpointConfig = new CheckpointConfig()): RDD[L] =
    withCount(
      t.setName("t").cache(),
      count,
      count / t.getNumPartitions,
      System.currentTimeMillis(),
      checkpointConfig
    )

  def withIndices(ti: RDD[(L, Long)],
                  count: Long,
                  checkpointConfig: CheckpointConfig): RDD[L] = {
    val startTime = System.currentTimeMillis()
    lastPrintedTime = startTime
    withIndices(
      ti,
      count,
      target = count / ti.getNumPartitions,
      startTime,
      checkpointConfig
    )
  }

  def withIndices(ti: RDD[(L, Long)],
                  n: L,
                  target: L,
                  startTime: Long,
                  checkpointConfig: CheckpointConfig): RDD[L] =
    saImpl(
      None,
      Some(ti),
      n,
      target,
      startTime,
      checkpointConfig
    )

  def withCount(t: RDD[L],
                n: L,
                target: L,
                startTime: Long,
                checkpointConfig: CheckpointConfig): RDD[L] =
    saImpl(
      Some(t),
      None,
      n,
      target,
      startTime,
      checkpointConfig
    )

  val debug = false

  def debugPrint(s: String) = {
    if (debug) {
      println(s)
    }
  }

  def progress[U:ClassTag](name: String, r: => RDD[U]): Unit = {
    if (debug) {
      val partitioned =
        r
          .mapPartitionsWithIndex((idx, iter) => iter.map(idx → _))
          .groupByKey
          .collect
          .sortBy(_._1)
          .map { case (partitionIdx, value) ⇒ s"$partitionIdx -> [ ${value.mkString(", ")} ]" }

      debugPrint(s"$name:\n\t${partitioned.mkString("\n\t")}\n")
    }
  }

  def since(start: Long): String = {
    val now = System.currentTimeMillis()
    val seconds = (now - start) / 1000
    if (seconds >= 60)
      s"${seconds / 60}m${seconds % 60}s"
    else
      s"${seconds}s"
  }

  var lastPrintedTime: Long = 0L

  def equivalentTuples(t1: L3, t2: L3): Boolean = {
    t1 == t2 &&
      t1._1 != 0L && t1._2 != 0L && t1._3 != 0L &&
      t2._1 != 0L && t2._2 != 0L && t2._3 != 0L
  }

  implicit val cmp2 = PairCmp
  implicit val cmp3 = TripletCmp
  implicit val cmpFn = JoinedCmp

  implicit val cmpL3I = new Ordering[L3I] {
    override def compare(x: L3I, y: L3I): PartitionIdx =
      cmp3.compare(
        (
          x._1._1,
          x._1._2,
          x._1._3,
          x._2
        ),
        (
          y._1._1,
          y._1._2,
          y._1._3,
          y._2
        )
      )
  }

  def saImpl(tOpt: Option[RDD[L]],
             tiOpt: Option[RDD[(L, Long)]],
             n: L,
             target: L,
             startTime: Long,
             checkpointConfig: CheckpointConfig): RDD[L] = {

    val sc = tOpt.orElse(tiOpt).get.context
    val fs = FileSystem.get(sc.hadoopConfiguration)

    val phaseStart = System.currentTimeMillis()

    val numDigits = n.toString.length

    // Modified scientific notation for the number of elements being processed on this pass; used in RDD names.
    // Sorted list of RDD names in the Spark UI is more reasonable this way.
    val N = s"e${numDigits - 1}·${n.toString.head}"

    def backupAny[U: ClassTag](name: String, fn: => U): U =
      checkpointConfig.backupPathOpt(name) match {
        case Some(bp) =>
          val pathStr = s"$bp/$n/$name"
          val path = new Path(pathStr)
          if (fs.exists(path)) {
            val ois = new ObjectInputStream(fs.open(path))
            val u = ois.readObject().asInstanceOf[U]
            ois.close()
            u
          } else {
            val oos = new ObjectOutputStream(fs.create(path))
            oos.writeObject(fn)
            oos.close()
            fn
          }
        case _ => fn
      }

    def backupRDD[U: ClassTag](name: String, fn: => RDD[U]): RDD[U] =
      (checkpointConfig.backupPathOpt(name) match {
        case Some(bp) =>
          val pathStr = s"$bp/$n/$name"
          val path = new Path(pathStr)
          val donePath = new Path(s"$pathStr.done")
          if (fs.exists(path) && fs.exists(donePath)) {
            sc.fromSequenceFile[U](path)
          } else {
            val rdd =
              fn
                .setName(s"$n-$name")
                .saveSequenceFile(
                  pathStr,
                  if (checkpointConfig.compressBackups(name))
                    Some(classOf[BZip2Codec])
                  else
                    None
                )

            fs.create(donePath).writeLong(1L)
            rdd
          }
        case _ =>
          fn
      })
      .setName(s"$N-$name")

    def pl(s: String): Unit = {
      println(s"${List(since(startTime), since(lastPrintedTime), since(phaseStart), s"$N ($n)").mkString("\t")} $s")
      lastPrintedTime = System.currentTimeMillis()
    }

    val numPartitions = tOpt.orElse(tiOpt).get.getNumPartitions

    pl(s"PDC3: $target ($n/$numPartitions)")

    lazy val t = tOpt.getOrElse(tiOpt.get.keys)

    if (n <= target || numPartitions == 1) {
      val r = t.map(_.toInt).collect()
      return t.context.parallelize(
        DC3.make(r, r.length).map(_.toLong)
      )
    }

    progress("SA", t)

    val ti = backupRDD("ti", tiOpt.getOrElse(t.zipWithIndex()))

    progress("ti", ti)

    val tuples: RDD[L3I] =
      if (n / t.getNumPartitions < 2)
        backupRDD(
          "tuples",
          (for {
            (e, i) <- ti
            j <- i-2 to i
            if j >= 0 && j % 3 != 0
          } yield
            (j, (e, i))
          )
          .setName(s"$N-flatmapped")
          .groupByKey()
          .setName(s"$N-tuples-grouped")
          .mapValues(
            _
              .toList
              .sortBy(_._2)
              .map(_._1) match {
                case e1 :: Nil => (e1, 0L, 0L)
                case e1 :: e2 :: Nil => (e1, e2, 0L)
                case es => (es(0), es(1), es(2))
              }
          )
          .setName(s"$N-list->tupled;zero-padded")
          .map(_.swap)
        )
      else
        backupRDD(
          "sliding",
          t
            .sliding3(0)
            .zipWithIndex
            .filter(_._2 % 3 != 0)
        )

    val padded =
      if (n % 3 == 0)
        tuples ++ t.context.parallelize(((0L, 0L, 0L), n+1) :: Nil)
      else if (n % 3 == 1)
        tuples ++ t.context.parallelize(((0L, 0L, 0L), n) :: Nil)
      else
        tuples ++ t.context.parallelize(((0L, 0L, 0L), n) :: Nil)

//    val padded =
//      if (n % 3 == 0)
//        tuples ++ t.context.parallelize(((0L, 0L, 0L), n+1) :: Nil)
//      else
//        tuples ++ t.context.parallelize(((0L, 0L, 0L), n) :: Nil)

    // All [12]%3 triplets and indexes, sorted.
    val S: RDD[L3I] = backupRDD("S", padded.sort())

    progress("S", S)

    def name(s: RDD[L3I], N: String): (Boolean, RDD[(L, Name)]) = {

      val namedTupleRDD: RDD[(Name, L3, L)] =
        s.mapPartitions { iter =>
          var prevTuples = ArrayBuffer[(Name, L3, L)]()
          var prevTuple: Option[(Name, L3, L)] = None

          iter.foreach { cur =>
            val (curTuple, curIdx) = cur
            val curName =
              prevTuple match {
                case Some((prevName, prevLastTuple, prevLastIdx)) =>
                  if (equivalentTuples(prevLastTuple, curTuple))
                    prevName
                  else
                    prevName + 1
                case None => 0L
              }

            prevTuple = Some((curName, curTuple, curIdx))
            prevTuples += ((curName, curTuple, curIdx))
          }

          prevTuples.toIterator
        }

      var foundDupes = false

      val named =
        backupRDD[(L, Long)](
          "named",
          {
            val lastTuplesRDD: RDD[NameTuple] =
              namedTupleRDD
                .mapPartitionsWithIndex {
                  (partitionIdx, iter) =>
                    if (iter.hasNext) {
                      var last: (Name, L3, L) = iter.next
                      val firstTuple = last._2

                      var len = 1
                      while (iter.hasNext) {
                        last = iter.next()
                        len += 1
                      }

                      val (lastName, lastTuple, _) = last

                      Array((partitionIdx, lastName, len.toLong, firstTuple, lastTuple)).toIterator
                    } else
                      Iterator()
                }

            val lastTuples = lastTuplesRDD.collect.sortBy(_._1)

            var partitionStartIdxs = ArrayBuffer[(PartitionIdx, Name)]()
            var prevEndCount = 1L
            var prevLastTupleOpt: Option[L3] = None

            for {
              (partitionIdx, curCount, partitionCount, curFirstTuple, curLastTuple) <- lastTuples
            } {
              val curStartCount =
                if (prevLastTupleOpt.exists(!equivalentTuples(_, curFirstTuple)))
                  prevEndCount + 1
                else
                  prevEndCount

              prevEndCount = curStartCount + curCount
              if (!foundDupes &&
                ((curCount + 1 != partitionCount) ||
                  prevLastTupleOpt.exists(equivalentTuples(_, curFirstTuple)))) {
                foundDupes = true
              }
              prevLastTupleOpt = Some(curLastTuple)
              partitionStartIdxs += ((partitionIdx, curStartCount))
            }

            foundDupes = backupAny[Boolean]("foundDupes", foundDupes)

            val partitionStartIdxsBroadcast = s.sparkContext.broadcast(partitionStartIdxs.toMap)

            namedTupleRDD
              .mapPartitionsWithIndex {
                (partitionIdx, iter) =>
                  partitionStartIdxsBroadcast.value.get(partitionIdx) match {
                    case Some(partitionStartName) =>
                      for {
                        (name, _, idx) <- iter
                      } yield
                        (idx, partitionStartName + name)
                    case _ =>
                      if (iter.nonEmpty)
                        throw new Exception(
                          List(
                            s"No partition start idxs found for partition $partitionIdx:",
                            partitionStartIdxsBroadcast.value.toList.map(p => s"${p._1} -> ${p._2}").mkString("\t", "\n\t", ""),
                            iter.take(100).mkString(",")
                          )
                          .mkString("\n\n")
                        )
                      else
                        Nil.toIterator
                  }
              }
          }
        )

      (foundDupes, named)
    }

    val n1 = (n + 1)/3 +
      (n % 3 match {
        case 0|1 => 1
        case 2 => 0
      })

    val n2 = (n + 1) / 3

    val P: RDD[(L, Name)] =
      backupRDD(
        "P",
        {
          val (foundDupes, named) = name(S, N)
          progress("named", named)

          if (foundDupes) {
            val onesThenTwos =
              backupRDD(
                "ones-then-twos",
                named
                  .map(p => ((p._1 % 3, p._1 / 3), p._2))
                  .setName(s"$N-mod-div-keyed")
                  .sortByKey(numPartitions = ((n1 + n2) / target).toInt)
                  .setName(s"$N-mod-div-sorted")
                  .values
              )

            progress("onesThenTwos", onesThenTwos)

            val SA12: RDD[L] =
              saImpl(
                Some(onesThenTwos),
                None,
                n1 + n2,
                target,
                startTime,
                checkpointConfig
              )
              .setName(s"$N-SA12")

            progress("SA12", SA12)

            SA12
            .zipWithIndex().setName(s"$N-SA12-zipped")
            .map(p => {
              (
                if (p._1 < n1)
                  3 * p._1 + 1
                else
                  3 * (p._1 - n1) + 2,
                p._2 + 1
              )
            }).setName(s"$N-indices-remapped")
            .sortByKey()
          } else {
            named.sortByKey()
          }
        }
      )

    progress("P", P)

    val keyedP: RDD[(L, Joined)] =
      backupRDD(
        "keyedP",
        (for {
          (idx, name) <- P
          i <- idx-2 to idx
          if i >= 0 && i < n
        } yield {
          val joined =
            (i % 3, idx - i) match {
              case (0, 1) => Joined(n0O = Some(name))
              case (0, 2) => Joined(n1O = Some(name))
              case (1, 0) => Joined(n0O = Some(name))
              case (1, 1) => Joined(n1O = Some(name))
              case (2, 0) => Joined(n0O = Some(name))
              case (2, 2) => Joined(n1O = Some(name))
              case _ => throw new Exception(s"Invalid (idx,i): ($idx,$i); $name")
            }

          i -> joined
        })
        .setName(s"$N-keyedP")
        .reduceByKey(Joined.merge _, numPartitions = t.getNumPartitions)
      )

    progress("keyedP", keyedP)

    val keyedT: RDD[(L, Joined)] =
      backupRDD(
        "keyedT",
        (for {
          (e, i) <- ti
          start = if (i % 3 == 2) i else i-1
          j <- start to i
          if j >= 0
        } yield {
          val joined =
            (j % 3, i - j) match {
              case (0, 0) => Joined(t0O = Some(e))
              case (0, 1) => Joined(t1O = Some(e))
              case (1, 0) => Joined(t0O = Some(e))
              case (2, 0) => Joined(t0O = Some(e))
              case (2, 1) => Joined(t1O = Some(e))
              case _ => throw new Exception(s"Invalid (i,j): ($i,$j); $e")
            }

          j -> joined
        })
        .setName(s"$N-keyedT")
        .reduceByKey(Joined.merge)
      )

    progress("keyedT", keyedT)

    val joined = backupRDD("joined", keyedT.join(keyedP).mapValues(Joined.mergeT))
    //val joined = (keyedT ++ keyedP).setName(s"$N-keyed-T+P").reduceByKey(Joined.merge).setName(s"$N-joined")

    progress("joined", joined)

    implicit val cmpFn = JoinedCmp
    val sorted = backupRDD("sorted", joined.sort().keys)
    progress("sorted", sorted)

    if (debug) {
      sorted
        .map(_ → null)
        .sortByKey()
        .keys
        .zipWithIndex
        .map(t ⇒
          if (t._1 != t._2)
            throw new Exception(s"idx ${t._2}: ${t._1}")
        )
        .count()
    }

    pl("Returning")
    sorted
  }
}

class PDC3Registrar extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Joined])
    kryo.register(classOf[Name])
    kryo.register(Class.forName("scala.reflect.ClassTag$$anon$1"))
    kryo.register(classOf[mutable.WrappedArray.ofRef[_]])
    kryo.register(classOf[mutable.WrappedArray.ofLong])
    kryo.register(classOf[mutable.WrappedArray.ofByte])
    kryo.register(classOf[java.lang.Class[_]])
    kryo.register(classOf[Array[String]])
    kryo.register(classOf[Array[Int]])
    kryo.register(Class.forName("scala.collection.immutable.Map$EmptyMap$"))
  }
}
