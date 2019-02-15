package it.trenzalore.tools.utils.string

object StringUtils {

  implicit class EnhancedString(s: String) {
    def toNullOrEmptyOption: Option[String] = if (s == null || s.isEmpty) None else Some(s)

    def removeSpaces: String = s.replaceAll(" ", "")
  }

}
