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
    def updateCount(char: Char, count: Int) = 
      if (char == '(') count + 1
      else if (char == ')') count - 1
      else count

    @tailrec
    def rec(chars: Array[Char], count: Int): Boolean = {
      if (chars.isEmpty) count == 0
      else {
        if (count < 0) false
        else rec(chars.tail, updateCount(chars.head, count))
      }
    }

    rec(chars, 0)
  }

  def updateCount(char: Char, openCount: Int, closedCount: Int): (Int, Int) = 
    if (char == '(')
      (openCount + 1, closedCount)
    else if (char == ')')
      if (openCount > 0) (openCount-1, closedCount)
      else (openCount, closedCount + 1)
    else 
      (openCount, closedCount)

  @tailrec
  def rec(chars: Array[Char], count: (Int, Int)): (Int, Int) = {
    if (chars.isEmpty) count
    else rec(chars.tail, updateCount(chars.head, count._1, count._2))
  }

  def delta(a: Int, b: Int) = {
    if (a > b) a - b else b - a
  }
  
  type Brackets = (Int, Int)
  def opened(b: Brackets) = b._1
  def closed(b: Brackets) = b._2
  
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

    def traverse(idx: Int, until: Int, arg1: Int, arg2: Int): (Int, Int) = {
      rec(chars.slice(idx, until), (0, 0))
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      if (until - from <= threshold)
        traverse(from, until, 0, 0)
      else {
        val mid = (until + from) / 2
        val (left, right) = parallel(
            traverse(from, mid, 0, 0), 
            traverse(mid, until, 0, 0))
         
         combine(left, right)
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
