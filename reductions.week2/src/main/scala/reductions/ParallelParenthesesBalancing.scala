package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    rec(chars, 0, chars.size, (0, 0)) == (0, 0)
  }

  def getBracketsCount(char: Char, openCount: Int, closedCount: Int): (Int, Int) = 
    if (char == '(')
      (openCount + 1, closedCount)
    else if (char == ')')
      if (openCount > 0) (openCount-1, closedCount)
      else (openCount, closedCount + 1)
    else 
      (openCount, closedCount)

  @tailrec
  def rec(chars: Array[Char], idx: Int, until: Int, count: (Int, Int)): (Int, Int) = {
    if (idx >= until) count
    else {
      rec(chars, idx+1, until, getBracketsCount(chars(idx), count._1, count._2))
    }
  }

  def combine(left: (Int, Int), right: (Int, Int)) = {
    val (leftOpen, leftClosed) = left
    val (rightOpen, rightClosed) = right

    if (leftOpen >= rightClosed) {
      val remainOpen = (leftOpen - rightClosed + rightOpen)
      val remainClosed = leftClosed
      (remainOpen, remainClosed)
    } else {
      val remainOpen = rightOpen
      val remainClosed = (rightClosed - leftOpen + leftClosed)
      (remainOpen, remainClosed)
    }
  }
  
  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, opened: Int, closed: Int): (Int, Int) = {
      rec(chars, idx, until, (opened, closed))
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      if (until - from <= threshold) 
        traverse(from, until, 0, 0)
      else {
        val mid = (until + from) / 2
        val (left, right) = parallel(
            reduce(from, mid), 
            reduce(mid, until))
         
         combine(left, right)
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
