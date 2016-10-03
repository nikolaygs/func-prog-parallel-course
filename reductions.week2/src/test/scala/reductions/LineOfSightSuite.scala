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

  test("downsweepSequential with from >= until") {
    val output = new Array[Float](4)
    downsweepSequential(Array[Float](0f, 1f, 8f, 9f), output, 0f, 4, 4)
    assert(output.toList == List(0f, 0f, 0f, 0f))
  }

  test("downsweepSequential should correctly handle a chunk of size 2 of an input array of size 5") {
    val output = new Array[Float](5)
    downsweepSequential(Array[Float](0f, 7f, 10f, 33f, 48f), output, 0f, 2, 4)
    assert(output.toList == List(0.0, 0.0, 5.0, 11.0, 0.0))
  }

  test("downsweepSequential (threshold = 1) should correctly handle a 4 element array when the starting angle is zero") {
    val input = Array[Float](0f, 1f, 8f, 9f)
    val output = new Array[Float](4)
    val tree = upsweep(input, 0, input.size, 10)
    val result = downsweepSequential(input, output, 1, 0, input.size)

    assert(output.toList == List(0f, 1f, 4f, 4f))
  }

  test("downsweep should correctly compute the output for a non-zero starting angle") {
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

