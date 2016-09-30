package reductions

import java.util.concurrent._
import scala.collection._
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import common._

import ParallelParenthesesBalancing._

@RunWith(classOf[JUnitRunner])
class ParallelParenthesesBalancingSuite extends FunSuite {

  test("balance should work for empty string") {
    def check(input: String, expected: Boolean) =
      assert(balance(input.toArray) == expected,
        s"balance($input) should be $expected")

    check("", true)
  }

  test("balance should work for string of length 1") {
    def check(input: String, expected: Boolean) =
      assert(balance(input.toArray) == expected,
        s"balance($input) should be $expected")

    check("(", false)
    check(")", false)
    check(".", true)
  }

  test("balance should work for string of length 2") {
    def check(input: String, expected: Boolean) =
      assert(balance(input.toArray) == expected,
        s"balance($input) should be $expected")

    check("()", true)
    check(")(", false)
    check("((", false)
    check("))", false)
    check(".)", false)
    check(".(", false)
    check("(.", false)
    check(").", false)
  }

  test("test updateCount") {
    def check(input: String, expected: (Int, Int)) =
      assert(rec(input.toCharArray, (0, 0)) == expected,
        s"balance($input) should be $expected")

    check("()", (0, 0))
    check(")(", (1, 1))
    check("((", (2, 0))
    check("))", (0, 2))
    check(".)", (0, 1))
    check(".(", (1, 0))
    check("(.", (1, 0))
    check(").", (0, 1))
    check("((()))", (0, 0))
    check("((())))", (0, 1))
    check("(((()))", (1, 0))
  }

  test("test combine") {
    assert(combine((1, 1), (1, 2)) == (1, 2))
    assert(combine((1, 0), (0, 3)) == (0, 2))
    assert(combine((0, 1), (1, 0)) == (1, 1))
    assert(combine((1, 0), (0, 1)) == (0, 0))
  }

  test("test combine associativity") {
    val x = combine((1, 1), (1, 2))
    val y = combine((1, 0), (0, 3))
    val z = combine((1, 0), (0, 1))
    val z1 = combine((0, 1), (1, 0))

    assert(combine(combine(x, y), z) == combine(x, combine(y, z)))
    assert(combine(combine(x, y), z1) == combine(x, combine(y, z1)))
  }

  test("test parBalance threshold == array.size") {
    def check(input: String, expected: Boolean) =
      assert(parBalance(input.toCharArray, input.length) == expected,
        s"balance($input) should be $expected")

    check("()", true)
    check(")(", false)
    check("((", false)
    check("))", false)
    check(".)", false)
    check(".(", false)
    check("(.", false)
    check(").", false)
    check("((()))", true)
    check("((())))", false)
    check("(((()))", false)
  }

  test("test parBalance threshold == 1") {
    def check(input: String, expected: Boolean) =
      assert(parBalance(input.toCharArray, 1) == expected,
        s"balance($input) should be $expected")

    check("()", true)
    check(")(", false)
    check("((", false)
    check("))", false)
    check(".)", false)
    check(".(", false)
    check("(.", false)
    check(").", false)
    check("((()))", true)
    check("((())))", false)
    check("(((()))", false)
  }

  test("test balance parallel 100000000 records") {
    val bigSize = 1000000
    val input = new Array[Char](bigSize)

    var i = 0
    while (i < bigSize) {
      val char = if (i%2 == 0) '(' else ')'
        input(i) = char
        i = i + 1
    }

    Console println "Char array initialized"
    
    def check(input: Array[Char], expected: Boolean) =
      assert(parBalance(input, 5000) == expected,
        s"balance($input) should be $expected")

    check(input, true)
  }

  test("test balance sequential 100000000 records") {
    val bigSize = 50000
    val input = new Array[Char](bigSize)

    var i = 0
    while (i < bigSize) {
      val char = if (i%2 == 0) '(' else ')'
        input(i) = char
        i = i + 1
    }

    Console println "Char array initialized"
    
    def check(input: Array[Char], expected: Boolean) =
      assert(balance(input) == expected,
        s"balance($input) should be $expected")

    check(input, true)
  }

}