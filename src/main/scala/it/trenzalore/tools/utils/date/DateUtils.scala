package it.trenzalore.tools.utils.date

import java.sql.{ Date, Timestamp }
import java.time.{ LocalDate, LocalDateTime, LocalTime }

import scala.annotation.tailrec

object DateUtils {

  implicit class LocalDateEnhanced(date: LocalDate) {

    def toSqlDate: Date = Date.valueOf(date)

    def toTimestamp: Timestamp = Timestamp.valueOf(LocalDateTime.of(date, LocalTime.MIDNIGHT))

    def toto: Int = 0

  }

  def listDatesBetween(date1: LocalDate, date2: LocalDate): Seq[LocalDate] = {
    @tailrec
    def loop(to: LocalDate, res: Seq[LocalDate]): Seq[LocalDate] = {
      if (res.last == to) {
        res
      } else {
        loop(to, res :+ res.last.plusDays(1))
      }
    }

    if (date1.isBefore(date2))
      loop(date2, Vector(date1))
    else
      loop(date1, Vector(date2))
  }

}
