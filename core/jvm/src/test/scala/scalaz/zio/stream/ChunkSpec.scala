package scalaz.zio.stream

import org.specs2._

class ChunkSpec extends Specification with ScalaCheck {
  def is = "ChunkSpec".title ^ s2"""
  chunk equality $chunkEquality
  chunk inequality $chunkInequality
  flatMap chunk $flatMapChunk
  filter chunk $filterChunk
  drop chunk $dropChunk
  drop singleton chunk $dropSingletonChunk
  take chunk $takeChunk
  take singleton chunk $takeSingletonChunk
  An Array-based chunk that is filtered empty and mapped must not throw NPEs. $nullArrayBug
  toArray on concat of a slice must work properly. $toArrayOnConcatOfSlice
  toArray on concat of empty and integers must work properly. $toArrayOnConcatOfEmptyAndInts
  """

  def chunkEquality =
    Chunk(1, 2, 3, 4, 5) must_===
      Chunk(1, 2, 3, 4, 5)

  def chunkInequality =
    Chunk(1, 2, 3, 4, 5) must_!==
      Chunk(1, 2, 3, 4, 5, 6)

  def flatMapChunk = {
    val c = Chunk.fromArray(Array(1, 2, 3, 4, 5))

    c.flatMap(i => Chunk(i, i)) must_===
      Chunk(1, 1, 2, 2, 3, 3, 4, 4, 5, 5)
  }

  def filterChunk =
    Chunk(1, 2, 3, 4, 5).filter(_ % 2 == 0) must_===
      Chunk(2, 4)

  def dropChunk =
    Chunk(1, 2, 3, 4, 5).drop(4) must_===
      Chunk(5)

  def dropSingletonChunk =
    Chunk(1).drop(4) must_=== Chunk.empty

  def takeChunk =
    Chunk(1, 2, 3, 4, 5).take(4) must_===
      Chunk(1, 2, 3, 4)

  def takeSingletonChunk =
    Chunk(1).take(4) must_=== Chunk(1)

  def nullArrayBug = {
    val c = Chunk.fromArray(Array(1, 2, 3, 4, 5))

    // foreach should not throw
    c.foreach(_ => ())

    c.filter(_ => false).map(_ * 2).length must_=== 0
  }

  def toArrayOnConcatOfSlice = {
    val onlyOdd: Int => Boolean = _ % 2 != 0
    val concat = Chunk(1, 1, 1).filter(onlyOdd) ++
      Chunk(2, 2, 2).filter(onlyOdd) ++
      Chunk(3, 3, 3).filter(onlyOdd)

    val array = concat.toArray

    array must_=== Array(1, 1, 1, 3, 3, 3)
  }

  def toArrayOnConcatOfEmptyAndInts = {
    val dest: Array[Int] = (Chunk.empty ++ Chunk.fromArray(Array(1, 2, 3))).toArray
    dest must_=== Array(1, 2, 3)
  }
}
