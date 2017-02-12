package org.hammerlab.suffixes.pdc3

import org.hammerlab.suffixes.pdc3.PDC3.OL

case class Joined(t0O: OL = None,
                  t1O: OL = None,
                  n0O: OL = None,
                  n1O: OL = None) {
  override def toString: String =
    s"J(${
      Seq(t0O, t1O, n0O, n1O)
        .map(_.getOrElse(" "))
        .mkString(",")
    })"
}

object Joined {
  def merge(j1: Joined, j2: Joined): Joined = {
    def get[T](fn: Joined ⇒ Option[T]): Option[T] =
      (fn(j1), fn(j2)) match {
        case (Some(f1), Some(f2)) =>
          throw new Exception(s"Merge error: $j1 $j2")
        case (f1O, f2O) => f1O.orElse(f2O)
      }

    Joined(
      get(_.t0O),
      get(_.t1O),
      get(_.n0O),
      get(_.n1O)
    )
  }

  def mergeT(t: (Joined, Joined)): Joined = merge(t._1, t._2)
}

