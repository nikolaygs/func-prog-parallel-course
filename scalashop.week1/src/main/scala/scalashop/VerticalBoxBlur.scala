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
  def blur(src: Img, dst: Img, from: Int, end: Int, radius: Int): Unit = {
    for {
      x <- (from until end)
      y <- (0 until src.height)
    } dst(x, y) = boxBlurKernel(src, x, y, radius)
  }

  /** Blurs the columns of the source image in parallel using `numTasks` tasks.
   *
   *  Parallelization is done by stripping the source image `src` into
   *  `numTasks` separate strips, where each strip is composed of some number of
   *  columns.
   */
  def parBlur(src: Img, dst: Img, numTasks: Int, radius: Int): Unit = {
    assert(numTasks > 0, "numTasks must be at least 1")

    val isDivisor = src.width%numTasks == 0

    val step = if (isDivisor) src.width/numTasks else src.width/numTasks + 1
    val beginIndeces = (0 to src.width) by step

    val chunks = beginIndeces zip {
      if (isDivisor) beginIndeces.tail
      else (beginIndeces.tail ++ (src.width to src.width))
    }
    
    // create task foreach chunk in the tail, the head will be executed in this thread
    val tasks = chunks.tail map {
      case (from, end) => task {
        blur(src, dst, from, end, radius)
      }
    }

    // compute the 'head' element in this thread
    val (from, end) = chunks.head 
    blur(src, dst, from, end, radius)

    // join the remaining tasks
    tasks foreach { _.join() }
  }

}
