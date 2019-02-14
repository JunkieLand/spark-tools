import it.trenzalore.build.Dependencies
import it.trenzalore.build.BuildDefinition

lazy val sparkUtils = RootProject(file("../spark-utils"))

lazy val sparkSourceManager = BuildDefinition
  .library("spark-source-manager")
  .settings(
    libraryDependencies ++= (Dependencies.sparkDependencies(isProvided = true) ++ Vector(
      Dependencies.enumeratum,
      Dependencies.ficus
    ))
  )
  .dependsOn(sparkUtils)