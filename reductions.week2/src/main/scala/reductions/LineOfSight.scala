package reductions

import org.scalameter._
import common._
import scala.annotation.tailrec

object LineOfSightRunner {
  
  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 100,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]) {
    val length = 10000000
    val input = (0 until length).map(_ % 100 * 1.0f).toArray
    val output = new Array[Float](length + 1)
    val seqtime = standardConfig measure {
      LineOfSight.lineOfSight(input, output)
    }
    println(s"sequential time: $seqtime ms")

    val partime = standardConfig measure {
      LineOfSight.parLineOfSight(input, output, 10000)
    }
    println(s"parallel time: $partime ms")
    println(s"speedup: ${seqtime / partime}")
  }
}

object LineOfSight {

  def max(a: Float, b: Float): Float = if (a > b) a else b

  def lineOfSight(input: Array[Float], output: Array[Float], maxAngle: Float = 0): Unit =
    lineOfSightInt(input, 0, input.size, output, maxAngle)

  private def lineOfSightInt(input: Array[Float], from: Int, until: Int,
      output: Array[Float], maxAngle: Float = 0): Unit = {
    if (until - from == 0)
      return

    // output(0) should always be '0'
    var i = if (from > 0) from else {output(0) = 0; 1}
    var max = 0f

    while (i < until) {
      val tangent = (input(i) / i)
      if (tangent > max) max = tangent

      output(i - from) = max
      i = i + 1
    }
  }

  sealed abstract class Tree {
    def maxPrevious: Float
  }

  case class Node(left: Tree, right: Tree) extends Tree {
    val maxPrevious = max(left.maxPrevious, right.maxPrevious)
  }

  case class Leaf(from: Int, until: Int, maxPrevious: Float) extends Tree

  /** Traverses the specified part of the array and returns the maximum angle.
   */
  def upsweepSequential(input: Array[Float], from: Int, until: Int): Float = {
    val output = new Array[Float](until-from)
    lineOfSightInt(input, from, until, output)

    @tailrec
    def findMax(output: Array[Float], from: Int, until: Int, currentMax: Float): Float = {
      if (from == until) currentMax
      else {
        val max = if (output(from) > currentMax) output(from) else currentMax
        findMax(output, from+1, until, max)
      }
    }

    findMax(output, 0, output.size, 0)
  }

  /** Traverses the part of the array starting at `from` and until `end`, and
   *  returns the reduction tree for that part of the array.
   *
   *  The reduction tree is a `Leaf` if the length of the specified part of the
   *  array is smaller or equal to `threshold`, and a `Node` otherwise.
   *  If the specified part of the array is longer than `threshold`, then the
   *  work is divided and done recursively in parallel.
   */
  def upsweep(input: Array[Float], from: Int, end: Int,
    threshold: Int): Tree = {
    if (end - from <= threshold)
      Leaf(from, end, upsweepSequential(input, from, end))
    else {
      val mid = (end + from) / 2
      val (left, right) = parallel(
            upsweep(input, from, mid, threshold), 
            upsweep(input, mid, end, threshold))

      Node(left, right)
    }

  }

  /** Traverses the part of the `input` array starting at `from` and until
   *  `until`, and computes the maximum angle for each entry of the output array,
   *  given the `startingAngle`.
   */
  def downsweepSequential(input: Array[Float], output: Array[Float],
    startingAngle: Float, from: Int, until: Int): Unit = {
    lineOfSight(input, output, startingAngle)
  }

  /** Pushes the maximum angle in the prefix of the array to each leaf of the
   *  reduction `tree` in parallel, and then calls `downsweepTraverse` to write
   *  the `output` angles.
   */
  def downsweep(input: Array[Float], output: Array[Float], startingAngle: Float,
    tree: Tree): Unit = tree match {
    case Leaf(from, until, maxPrevious) => downsweepSequential(input, output, maxPrevious, from, until)
    case Node(left, right) => {
      parallel(
          downsweep(input, output, startingAngle, left),
          downsweep(input, output, startingAngle, right))
    }
  }

  /** Compute the line-of-sight in parallel. */
  def parLineOfSight(input: Array[Float], output: Array[Float],
    threshold: Int): Unit = {
    val from = 0
    val end = input.size

    val resTree = upsweep(input, from, end, threshold)
    downsweep(input, output, 0f, resTree)
  }
}
