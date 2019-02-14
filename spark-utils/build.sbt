import it.trenzalore.build.Dependencies
import it.trenzalore.build.BuildDefinition

lazy val sparkUtils = BuildDefinition
  .library("spark-utils")
  .settings(
    libraryDependencies ++= (Dependencies.sparkDependencies(isProvided = true) ++ Vector(
      Dependencies.typesafeConfig,
      Dependencies.scalaLogging
    ))
  )
