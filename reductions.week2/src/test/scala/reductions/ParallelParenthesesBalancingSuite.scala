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

//    check("((", false)
//    check("))", false)
//    check(".)", false)
//    check(".(", false)
//    check("(.", false)
//    check(").", false)
  }

}