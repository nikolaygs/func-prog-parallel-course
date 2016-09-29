
import common._

package object scalashop {

  /** The value of every pixel is represented as a 32 bit integer. */
  type RGBA = Int

  /** Returns the red component. */
  def red(c: RGBA): Int = (0xff000000 & c) >>> 24

  /** Returns the green component. */
  def green(c: RGBA): Int = (0x00ff0000 & c) >>> 16

  /** Returns the blue component. */
  def blue(c: RGBA): Int = (0x0000ff00 & c) >>> 8

  /** Returns the alpha component. */
  def alpha(c: RGBA): Int = (0x000000ff & c) >>> 0

  /** Used to create an RGBA value from separate components. */
  def rgba(r: Int, g: Int, b: Int, a: Int): RGBA = {
    (r << 24) | (g << 16) | (b << 8) | (a << 0)
  }

  /** Restricts the integer into the specified range. */
  def clamp(v: Int, min: Int, max: Int): Int = {
    if (v < min) min
    else if (v > max) max
    else v
  }

  /** Image is a two-dimensional matrix of pixel values. */
  class Img(val width: Int, val height: Int, private val data: Array[RGBA]) {
    def this(w: Int, h: Int) = this(w, h, new Array(w * h))
    def apply(x: Int, y: Int): RGBA = data(y * width + x)
    def update(x: Int, y: Int, c: RGBA) = data(y * width + x) = c
  }

  /** Computes the blurred RGBA value of a single pixel of the input image. */
  def boxBlurKernel(src: Img, x: Int, y: Int, radius: Int, print: Boolean = true): RGBA = {
    val minX = clamp(x - radius, 0, src.width-1)
    val maxX = clamp(x + radius, 0, src.width-1)
    val minY = clamp(y - radius, 0, src.height-1)
    val maxY = clamp(y + radius, 0, src.height-1)

    // get the RGBA of the pixels in radius
    val pixelsRGBA = 
      for {
        x <- (minX to maxX)
        y <- (minY to maxY) 
      } yield src(x, y)

    // get the cumulative value of each rgba element for the selected pixels 
    val (r, g, b, a) = pixelsRGBA.foldLeft(0, 0, 0, 0) { 
      case ((sumR, sumG, sumB, sumA), rgba) =>
        (sumR + red(rgba), sumG + green(rgba), sumB + blue(rgba), sumA + alpha(rgba))
    }

    val count = pixelsRGBA.size
    val avg = (total: Int) => total / count

    rgba(avg(r), avg(g), avg(b), avg(a))
  }

  def parBlurInternal(src: Img, dst: Img, matrixSize: Int, numTasks: Int, radius: Int, 
      blurF: (Img, Img, Int, Int, Int) => Unit): Unit = {
    // get the chunks interval for each parallel task
    val chunks = getChunks(numTasks, matrixSize)

    // schedule tasks for each chunk that will run into parallel thread
    val tasks = chunks map {
      case (from, end) => task {
        blurF(src, dst, from, end, radius)
      }
    }

    // calculate the head in the current thread
    tasks.head.get

    // wait the asynchonous computation of the remaining ones
    tasks.tail foreach { _.join() }
  }

  def getChunks(numTasks: Int, matrixSize: Int) = {
    // each column will be processed into separate task
    if (numTasks > matrixSize) for (i <- 0 until matrixSize) yield (i, i+1)
    // each task will process exactly the same column numbers (step value)
    else if (matrixSize%numTasks == 0) {
      val step = matrixSize / numTasks
      val beginIndeces = (0 to matrixSize) by step
      beginIndeces zip beginIndeces.tail
    }
    // if the numTaks in not divisor of matrix size, the first tasks (== remainder size) will process 
    // one additional row/column besides the calculated step. The remaining ones (second list) will process 1 step
    else {
      val remainder = matrixSize % numTasks
      val step = matrixSize / numTasks

      val first = (0 to remainder) map (_ * (step + 1)) toList
      val secondIdx = (remainder * (step + 1))
      val second = (secondIdx to matrixSize) map (_ * step) toList

      (first zip (first.tail)) ::: (second zip second.tail)
    }
  }

}
