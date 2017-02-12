package org.hammerlab.suffixes.pdc3

import org.hammerlab.suffixes.pdc3.PDC3.{ L, L3, L3I, Name, equivalentTuples }

/**
 * Wrap an iterator
 */
object NamingIterator {
  implicit class NamingIterator(val it: Iterator[L3I])
    extends AnyVal {

    def name() =
      new Iterator[(Name, L3, L)] {

        var prevTuple: Option[(Name, L3, L)] = None

        override def hasNext: Boolean = it.hasNext

        override def next(): (Name, (L, L, L), L) = {
          val (curTuple, curIdx) = it.next()
          val curName =
            prevTuple match {
              case Some((prevName, prevLastTuple, _)) =>
                if (equivalentTuples(prevLastTuple, curTuple))
                  prevName
                else
                  prevName.next
              case None => Name(0)
            }

          prevTuple = Some((curName, curTuple, curIdx))

          (curName, curTuple, curIdx)
        }
      }
  }
}
