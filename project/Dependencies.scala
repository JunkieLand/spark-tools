import sbt._

object Versions {
  val scalaVersion = "2.11.12"
  val sbtVersion = "0.13.17"
  val sparkVersion = "2.3.0"
  val derbyVersion = "10.14.2.0"
  val pac4jVersion = "2.2.1"
  val hbaseVersion = "1.3.1"
}

object Dependencies {

  import Versions._

  // The repositories where to download packages (jar) from
  val resolvers = Seq()

  val jodaTime = "joda-time" % "joda-time" % "2.9.6"
  val enumeratum = "com.beachape" %% "enumeratum" % "1.5.12"
  val typesafeConfig = "com.typesafe" % "config" % "1.3.2"
  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"
  val ficus = "com.iheart" %% "ficus" % "1.4.3"

  val commonLibraries = Seq(
    "org.scalatest" %% "scalatest" % "3.0.5" % "test",
    "org.scalamock" %% "scalamock" % "4.1.0" % "test"
  )

  def sparkDependencies(isProvided: Boolean) = {
    val scope = if (isProvided) "provided" else "compile"

    Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % scope,
      "org.apache.spark" %% "spark-sql" % sparkVersion % scope
    // "org.apache.spark" %% "spark-hive" % sparkVersion % scope
    )
  }

}