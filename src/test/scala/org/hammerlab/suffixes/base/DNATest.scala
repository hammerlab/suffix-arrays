package org.hammerlab.suffixes.base

trait DNATest extends Base {
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
