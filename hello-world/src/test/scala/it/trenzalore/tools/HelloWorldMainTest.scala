package it.trenzalore.tools

import org.scalatest.{ FunSuite, GivenWhenThen, Matchers }

class HelloWorldMainTest extends FunSuite with Matchers with GivenWhenThen {

  test("Hello World should say Hi !") {
    Given("a name")
    val name = "World"

    When("saying hi")
    val greetings = HelloWorldMain.sayHi(name)

    Then("World should be greeted")
    greetings should be("Hello World !")
  }

}
