package it.trenzalore.utils.date

import java.time.LocalDate

import org.scalatest.{ FunSuite, GivenWhenThen, Matchers }

class DateUtilsTest extends FunSuite with Matchers with GivenWhenThen {

  test("listDatesBetween should list dates in ascending order") {
    Given("date1 before date2")
    val date1 = LocalDate.of(2018, 1, 1)
    val date2 = LocalDate.of(2018, 1, 4)

    When("listing dates between them")
    val res = DateUtils.listDatesBetween(date1, date2)

    Then("all dates between date1 and date2 should be there including those")
    res should contain(LocalDate.of(2018, 1, 1))
    res should contain(LocalDate.of(2018, 1, 2))
    res should contain(LocalDate.of(2018, 1, 3))
    res should contain(LocalDate.of(2018, 1, 4))
    res.size should be(4)
  }

  test("listDatesBetween should list dates in descending order") {
    Given("date1 before date2")
    val date1 = LocalDate.of(2018, 1, 4)
    val date2 = LocalDate.of(2018, 1, 1)

    When("listing dates between them")
    val res = DateUtils.listDatesBetween(date1, date2)

    Then("all dates between date1 and date2 should be there including those")
    res should contain(LocalDate.of(2018, 1, 1))
    res should contain(LocalDate.of(2018, 1, 2))
    res should contain(LocalDate.of(2018, 1, 3))
    res should contain(LocalDate.of(2018, 1, 4))
    res.size should be(4)
  }

  test("listDatesBetween should list dates if start and end are the same") {
    Given("date1 before date2")
    val date1 = LocalDate.of(2018, 1, 1)
    val date2 = LocalDate.of(2018, 1, 1)

    When("listing dates between them")
    val res = DateUtils.listDatesBetween(date1, date2)

    Then("all dates between date1 and date2 should be there including those")
    res should contain(LocalDate.of(2018, 1, 1))
    res.size should be(1)
  }

}
