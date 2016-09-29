package scalashop

import org.scalameter._
import common._

object VerticalBoxBlurRunner {

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 5,
    Key.exec.maxWarmupRuns -> 10,
    Key.exec.benchRuns -> 10,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val radius = 3
    val width = 1920
    val height = 1080
    val src = new Img(width, height)
    val dst = new Img(width, height)
    val seqtime = standardConfig measure {
      VerticalBoxBlur.blur(src, dst, 0, width, radius)
    }
    println(s"sequential blur time: $seqtime ms")

    val numTasks = 32
    val partime = standardConfig measure {
      VerticalBoxBlur.parBlur(src, dst, numTasks, radius)
    }
    println(s"fork/join blur time: $partime ms")
    println(s"speedup: ${seqtime / partime}")
  }

}

/** A simple, trivially parallelizable computation. */
object VerticalBoxBlur {

  /** Blurs the columns of the source image `src` into the destination image
   *  `dst`, starting with `from` and ending with `end` (non-inclusive).
   *
   *  Within each column, `blur` traverses the pixels by going from top to
   *  bottom.
   */  
  /** Blurs the rows of the source image `src` into the destination image `dst`,
   *  starting with `from` and ending with `end` (non-inclusive).
   *
   *  Within each row, `blur` traverses the pixels by going from left to right.
   */
  def blur(src: Img, dst: Img, from: Int, end: Int, radius: Int, print: Boolean = false): Unit = {
    for {
      y <- (from until end)
      x <- (0 until src.width)
    } {
      if (print)
        Console println s"Task#${from} modifies (${x}, ${y})"

      dst(x, y) = boxBlurKernel(src, x, y, radius)
    }
  }

  /** Blurs the rows of the source image in parallel using `numTasks` tasks.
   *
   *  Parallelization is done by stripping the source image `src` into
   *  `numTasks` separate strips, where each strip is composed of some number of
   *  rows.
   */
  def parBlur(src: Img, dst: Img, numTasks: Int, radius: Int): Unit = {
    Console println "======================================================="
    assert(numTasks > 0, "numTasks must be at least 1")

    val isDivisor = src.height % numTasks == 0

    val step = if (isDivisor) src.height / numTasks else src.height / numTasks + 1
    val beginIndeces = (0 to src.height) by step
    
    val chunks = beginIndeces zip {
      if (isDivisor) beginIndeces.tail
      else (beginIndeces.tail ++ (src.height to src.height))
    }

    Console println s"${chunks}"
    val tasks = chunks.tail map {
      case (from, end) => {
//        Console println s"Starting new task: ${from} to ${end}"
        task {
          blur(src, dst, from, end, radius)
        }
      }
    }

    // compute the 'head' element in this thread
    val (from, end) = chunks.head
    Console println s"Starting new task: ${from} to ${end}"
    blur(src, dst, from, end, radius, true)

    // join the remaining ones
    tasks foreach { _.join() }
    Console println "======================================================="
  }

}
