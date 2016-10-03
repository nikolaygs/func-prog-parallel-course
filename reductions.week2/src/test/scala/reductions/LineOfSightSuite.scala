package reductions

import java.util.concurrent._
import scala.collection._
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import common._
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory

@RunWith(classOf[JUnitRunner]) 
class LineOfSightSuite extends FunSuite {
  import LineOfSight._
  test("lineOfSight should correctly handle an array of size 4") {
    val output = new Array[Float](4)
    lineOfSight(Array[Float](0f, 1f, 8f, 9f), output)
    assert(output.toList == List(0f, 1f, 4f, 4f))
  }


  test("upsweepSequential should correctly handle the chunk 1 until 4 of an array of 4 elements") {
    val res = upsweepSequential(Array[Float](0f, 1f, 8f, 9f), 1, 4)
    assert(res == 4f)
  }


  test("upsweep parallel (threshold = 10) should correctly handle the chunk 1 until 4 of an array of 4 elements") {
    val result = upsweep(Array[Float](0f, 1f, 8f, 9f), 1, 4, 10)
    assert(result.maxPrevious == 4)
  }

  test("upsweep parallel (threshold = 1) should correctly handle the chunk 1 until 4 of an array of 4 elements") {
    val result = upsweep(Array[Float](0f, 1f, 8f, 9f), 1, 4, 1)
    assert(result.maxPrevious == 4)
  }

  test("downsweepSequential should correctly handle a 4 element array when the starting angle is zero") {
    val output = new Array[Float](4)
    downsweepSequential(Array[Float](0f, 1f, 8f, 9f), output, 0f, 1, 4)
    assert(output.toList == List(0f, 1f, 4f, 4f))
  }

  test("downsweepSequential (threshold = 1) should correctly handle a 4 element array when the starting angle is zero") {
    val input = Array[Float](0f, 1f, 8f, 9f)
    val output = new Array[Float](4)
    val tree = upsweep(input, 0, input.size, 10)
    val result = downsweepSequential(input, output, 1, 0, input.size)

    assert(output.toList == List(0f, 1f, 4f, 4f))
  }

  test("parLineOfSight (threshold = 10) should correctly handle a 4 element array when the starting angle is zero") {
    val input = Array[Float](0f, 1f, 8f, 9f)
    val output = new Array[Float](4)
    parLineOfSight(input, output, 10)

    assert(output.toList == List(0f, 1f, 4f, 4f))
  }

  test("parLineOfSight (threshold = 1) should correctly handle a 4 element array when the starting angle is zero") {
    val input = Array[Float](0f, 1f, 8f, 9f)
    val output = new Array[Float](4)
    parLineOfSight(input, output, 1)

    assert(output.toList == List(0f, 1f, 4f, 4f))
  }

}

