package org.hammerlab.suffixes.pdc3

import java.io.{ ObjectInputStream, ObjectOutputStream }

import com.esotericsoftware.kryo.Kryo
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.rdd.RDD
import org.hammerlab.iterator.NextOptionIterator
import org.hammerlab.magic.rdd.serde.SequenceFileSerializableRDD._
import org.hammerlab.magic.rdd.sliding.SlidingRDD._
import org.hammerlab.magic.rdd.sort.SortRDD._
import org.hammerlab.magic.rdd.zip.LazyZippedWithIndexRDD._
import org.hammerlab.magic.rdd.zip.ZipPartitionsWithIndexRDD._
import org.hammerlab.spark.PartitionIndex
import org.hammerlab.suffixes.dc3.DC3
import org.hammerlab.suffixes.pdc3.NamingIterator._

import scala.collection.mutable
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

  implicit class Name(val name: Long) extends AnyVal {
    def next: Name = name + 1
    def +(other: Name): Name = name + other.name
    override def toString: String = name.toString
  }

  object Name {
    def unapply(name: Name): Option[Long] = Some(name.name)
  }

  type L3 = (L, L, L)
  type L3I = (L3, L)
  type NameTuple = (PartitionIndex, Name, L, L3, L3)
  type OL = Option[L]

  def apply(t: RDD[L]): RDD[L] = apply(t, t.count)

  def apply(t: RDD[L],
            count: Long,
            checkpointConfig: CheckpointConfig = new CheckpointConfig()): RDD[L] =
    saImpl(
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
    saImpl(
      ti,
      count,
      target = count / ti.getNumPartitions,
      startTime,
      checkpointConfig
    )
  }

  val debug = false

  def debugPrint(s: String) = {
    if (debug) {
      println(s)
    }
  }

  def progress[U:ClassTag](name: String, r: ⇒ RDD[U]): Unit = {
    if (debug) {
      val partitioned =
        r
          .mapPartitionsWithIndex((idx, iter) ⇒ iter.map(idx → _))
          .groupByKey
          .collect
          .sortBy(_._1)
          .map {
            case (partitionIdx, value) ⇒
              s"$partitionIdx → [ ${value.mkString(", ")} ]"
          }

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
    override def compare(x: L3I, y: L3I): PartitionIndex =
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

  def saImpl(rdds: RDDPair[L],
             n: L,
             target: L,
             startTime: Long,
             checkpointConfig: CheckpointConfig): RDD[L] = {

    val RDDPair(t, ti) = rdds

    val sc = rdds.sc
    val fs = FileSystem.get(sc.hadoopConfiguration)

    val phaseStart = System.currentTimeMillis()

    val numDigits = n.toString.length

    // Modified scientific notation for the number of elements being processed on this pass; used in RDD names.
    // Sorted list of RDD names in the Spark UI is more reasonable this way.
    val N = s"e${numDigits - 1}·${n.toString.head}"

    def backupAny[U: ClassTag](name: String, fn: ⇒ U): U =
      checkpointConfig.backupPathOpt(name) match {
        case Some(bp) ⇒
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
        case _ ⇒ fn
      }

    def backupRDD[U: ClassTag](name: String, fn: ⇒ RDD[U]): RDD[U] =
      (checkpointConfig.backupPathOpt(name) match {
        case Some(bp) ⇒
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

            val donePathOut = fs.create(donePath)
            donePathOut.writeLong(1L)
            donePathOut.close()
            rdd
          }
        case _ ⇒
          fn
      })
      .setName(s"$N-$name")

    def pl(s: String): Unit = {
      println(s"${List(since(startTime), since(lastPrintedTime), since(phaseStart), s"$N ($n)").mkString("\t")} $s")
      lastPrintedTime = System.currentTimeMillis()
    }

    val numPartitions = rdds.numPartitions

    pl(s"PDC3: $target ($n/$numPartitions)")

    if (n <= target || numPartitions == 1) {
      val r = t.map(_.toInt).collect()
      return t.context.parallelize(
        DC3.make(r, r.length).map(_.toLong)
      )
    }

    progress("SA", t)

    // Form each element into a triplet with the two elements that follow it, zipWithIndex those, and drop the ones
    // whose index is 0 (mod 3).
    val tuples: RDD[L3I] =
      if (n / t.getNumPartitions < 2)
        // `sliding3` below currently throws when there are partitions with fewer than 2 elements, so in the case that
        // the number of elements is less than twice the number of partitions (meaning some partition necessarily has <2
        // elements) we do a full shuffle to get the "lagged self-join" that we seek.
        backupRDD(
          "tuples",
          (for {
            (e, i) ← ti
            j ← i-2 to i
            if j >= 0 && j % 3 != 0
          } yield
            j → (e, i)
          )
          .setName(s"$N-flatmapped")
          .groupByKey()
          .setName(s"$N-tuples-grouped")
          .mapValues(
            _
              .toList
              .sortBy(_._2)
              .map(_._1) match {
                case e1 :: Nil ⇒ (e1, 0L, 0L)
                case e1 :: e2 :: Nil ⇒ (e1, e2, 0L)
                case es ⇒ (es(0), es(1), es(2))
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
            .lazyZipWithIndex
            .filter(_._2 % 3 != 0)
        )

    // 0's are considered to be sentinels, with each one distinct and lexicographically ordered according to its
    // position in the collection. We want to make sure there is
    val padded =
      if (n % 3 == 0)
        tuples ++ t.context.parallelize(((0L, 0L, 0L), n+1) :: Nil)
      else if (n % 3 == 1)
        tuples ++ t.context.parallelize(((0L, 0L, 0L), n) :: Nil)
      else
        tuples ++ t.context.parallelize(((0L, 0L, 0L), n) :: Nil)

    // All [12]%3 triplets and indexes, sorted by `cmpL3I` above.
    val S: RDD[L3I] = backupRDD("S", padded.sort())

    progress("S", S)

    /**
     * "Name" the tuples in the RDD [[S]] above.
     *
     * "Name"s here are just integers, with the property that the ordering of the input elements corresponds to
     * increasing element-"name"s, and two elements' names are equal iff the elements themselves are equal.
     *
     * @param s input RDD: 3 consecutive elements from the top-level RDD [[t]], mapped to one RDD-index ([[Long]]).
     * @return Each element's initial RDD-index value from above, as well as its "name" (one [[Long]] that represents
     *         its [[Tuple3]]'s place in an ordering of all the RDD-elements' [[Tuple3]]s.
     */
    def name(s: RDD[L3I]): (Boolean, RDD[(L, Name)]) = {

      val namedTupleRDD: RDD[(Name, L3, L)] = s.mapPartitions(it ⇒ new NamingIterator(it).name())

      var foundDupes = false

      val named =
        backupRDD[(L, Name)](
          "named",
          {
            // For each non-empty partition, collect to the driver a tuple containing:
            //
            //   - partition index
            //   - last "name" in that partition
            //   - partition size
            //   - first tuple
            //   - last tuple
            val lastTuples: Array[NameTuple] =
              namedTupleRDD
                .mapPartitionsWithIndex(
                  (partitionIdx, iter) ⇒
                    if (iter.hasNext) {
                      var last: (Name, L3, L) = iter.next
                      val firstTuple = last._2

                      var len = 1
                      while (iter.hasNext) {
                        last = iter.next()
                        len += 1
                      }

                      val (lastName, lastTuple, _) = last

                      Iterator((partitionIdx, lastName, len.toLong, firstTuple, lastTuple))
                    } else
                      Iterator()
                )
                .collect

            val partitionStartIdxsMap = mutable.Map[PartitionIndex, Name]()

            // First name
            var prevEndName = Name(1L)
            var prevLastTupleOpt: Option[L3] = None

            for {
              (partitionIdx, lastName, partitionSize, firstTuple, lastTuple) ← lastTuples
            } {
              val partitionStartName =
                if (prevLastTupleOpt.exists(!equivalentTuples(_, firstTuple)))
                  prevEndName.next
                else
                  prevEndName

              prevEndName = partitionStartName + lastName
              if (!foundDupes &&
                ((lastName.next.name != partitionSize) ||
                  prevLastTupleOpt.exists(equivalentTuples(_, firstTuple)))) {
                foundDupes = true
              }
              prevLastTupleOpt = Some(lastTuple)
              partitionStartIdxsMap(partitionIdx) = partitionStartName
            }

            foundDupes = backupAny[Boolean]("foundDupes", foundDupes)

            // RDD with one element per non-empty partition, to zip-distribute.
            val partitionStartIdxsRDD =
              sc.parallelize(
                (0 until namedTupleRDD.getNumPartitions)
                  .map(partitionStartIdxsMap.get),
                numSlices = namedTupleRDD.getNumPartitions
              )

            namedTupleRDD
              .zipPartitionsWithIndex(partitionStartIdxsRDD)(
                (partitionIdx, iter, startIdxIter) ⇒
                  (iter.hasNext, startIdxIter.nextOption.flatten) match {
                      case (_, Some(partitionStartName)) ⇒
                        for {
                          (name, _, idx) ← iter
                        } yield
                          (idx, partitionStartName + name)
                      case (true, _) ⇒
                        throw new Exception(
                          List(
                            s"No partition start idxs found for partition $partitionIdx:",
                            iter.take(100).mkString(",")
                          )
                          .mkString("\n\n")
                        )
                      case _ ⇒
                          Iterator()
                    }
              )
          }
        )

      (foundDupes, named)
    }

    // The number of elements whose index is 1 (mod 3).
    val n1 = (n + 1)/3 +
      (n % 3 match {
        case 0|1 ⇒ 1
        case 2 ⇒ 0
      })

    // The number of elements whose index is 2 (mod 3).
    val n2 = (n + 1) / 3

    /**
     * [[P]] will have all [12]%3-indices from [[t]], paired with their suffix-ranks (1 through ([[n1]]+[[n2]]))
     * relative to the others in that set.
     */
    val P: RDD[(L, Name)] =
      backupRDD(
        "P",
        {
          val (foundDupes, named) = name(S)
          progress("named", named)

          if (!foundDupes)
            named.sortByKey()
          else {
            /**
             *
             * If there are duplicates in the triplet-RDD, we'll have to recurse on triplets of the triplet-names
             * (equivalent to moving to comparing 9-element subsequences anchored at each element).
             *
             * To start, re-sort the [[named]] RDD (containing all [12]%3 indices and their [[Name]]s) so that all 1%3
             * indices are shifted to occupy the first contiguous half of the new RDD, and the 2%3 indices occupy the
             * second half (preserving the relative order among each half's elements).
             */
            val onesThenTwos =
              backupRDD(
                "ones-then-twos",
                named
                  .map {
                    case (idx, name) ⇒
                      // This [[Tuple2]] key is introduced only for sorting below, putting all 1%3's ahead of all 2%3's
                      // (and preserving each group's internal order), and then dropped.
                      (idx % 3, idx / 3) →
                        (
                          name.name,
                          // After the sort described above, this will be the index of this element, saving us a
                          // `zipWithIndex` later.
                          if (idx % 3 == 1)
                            idx / 3
                          else
                            n1 + (idx / 3)
                        )
                  }
                  .setName(s"$N-mod-div-keyed")
                  .sortByKey(numPartitions = ((n1 + n2) / target).toInt)
                  .setName(s"$N-mod-div-sorted")
                  .values
              )

            progress("onesThenTwos", onesThenTwos)

            /**
             * [[SA12]] is the suffix-array of [[onesThenTwos]]; the former's values are a permutation of the latter's
             * indices.
             */
            val SA12: RDD[L] =
              saImpl(
                RDDPair(onesThenTwos),
                n1 + n2,
                target,
                startTime,
                checkpointConfig
              )
              .setName(s"$N-SA12")

            progress("SA12", SA12)

            SA12
              .lazyZipWithIndex
              .setName(s"$N-SA12-zipped")
              .map {
                /** For each [[onesThenTwos]] index and its suffix-rank… */
                case (ottIdx, rank) ⇒

                  /**
                   * Map the [[onesThenTwos]] index in back to an index from the original ([[t]]/[[ti]]) array:
                   *
                   *   - the first [[n1]] elements of [[onesThenTwos]] were the 1%3 indices from [[t]]
                   *   - the latter [[n2]] elements of [[onesThenTwos]] were the 2%3 indices from [[t]].
                   */
                  val originalIdx =
                    if (ottIdx < n1)
                      3 * ottIdx + 1
                    else
                      3 * (ottIdx - n1) + 2

                  /** Include 1-based [[Name]]s (suffix-ranks) with each [[t]]-index above. */
                  val name = Name(rank + 1)

                  originalIdx → name
              }
              .setName(s"$N-indices-remapped")
              .sortByKey()
          }
        }
      )

    progress("P", P)

    /**
     * Construct an [[RDD]] where every index `I` from the original [[RDD]] is mapped to a [[Joined]] with the two
     * succeeding named [12]%3 elements (starting with the one at `I`, in the case that `I` is 1 or 2 (mod 3).
     */
    val keyedP: RDD[(L, Joined)] =
      backupRDD(
        "keyedP",
        (for {
          /**
           * For each [12]%3 index and its [[Name]] (which orders its suffix relative to those of all other [12]%3
           * indices).
           */
          (idx, Name(name)) ← P

          /** The current [[Name]] may be relative from two elements back through the current position. */
          i ← idx-2 to idx

          /** Only emit data at valid indices. */
          if i >= 0 && i < n
        } yield {
          val joined =
            (i % 3, idx - i) match {
              // The succeeding two named elements from 0%3 indices are 1 and 2 spaces ahead.
              case (0, 1) ⇒ Joined(n0O = Some(name))
              case (0, 2) ⇒ Joined(n1O = Some(name))

              // The succeeding two named elements from 1%3 indices are the current and next elements.
              case (1, 0) ⇒ Joined(n0O = Some(name))
              case (1, 1) ⇒ Joined(n1O = Some(name))

              // The succeeding two named elements from 2%3 indices are the current and 2-ahead elements.
              case (2, 0) ⇒ Joined(n0O = Some(name))
              case (2, 2) ⇒ Joined(n1O = Some(name))
              case _ ⇒ throw new Exception(s"Invalid (idx,i): ($idx,$i); $name")
            }

          i → joined
        })
        .setName(s"$N-keyedP")
        .reduceByKey(Joined.merge _, numPartitions = t.getNumPartitions)
      )

    progress("keyedP", keyedP)

    /**
     * Construct an [[RDD]] with every position's current and next element, with one exception: 1%3 indices don't need
     * the next element.
     */
    val keyedT: RDD[(L, Joined)] =
      backupRDD(
        "keyedT",
        (for {
          (e, i) ← ti

          /**
           * In general, we want to emit [[e]] keyed by [[i]] and [[i]]-1.
           *
           * However, 1%3 positions don't need the next element in order to compare themselves to any of the equivalence
           * classes (mod 3), so we don't need to emit [[e]] for [[i-1]] when [[i]] is 2%3.
           */
          joineds =
            if (i % 3 == 2 || i == 0)
              Seq(i → Joined(t0O = Some(e)))
            else
              Seq(i → Joined(t0O = Some(e)), (i - 1) → Joined(t1O = Some(e)))

          (idx, joined) ← joineds
        } yield
          idx → joined
        )
        .setName(s"$N-keyedT")
        .reduceByKey(Joined.merge)
      )

    progress("keyedT", keyedT)

    val joined =
      backupRDD(
        "joined",
        keyedT
          .join(keyedP)
          .mapValues(Joined.mergeT)
      )

//    val joined =
//      (keyedT ++ keyedP)
//        .setName(s"$N-keyed-T+P")
//        .reduceByKey(Joined.merge)
//        .setName(s"$N-joined")

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

  def register(kryo: Kryo): Unit = {
    kryo.register(classOf[Name])
  }
}


