import it.trenzalore.build.Dependencies
import it.trenzalore.build.BuildDefinition

lazy val sparkUtils = RootProject(file("../spark-utils"))

lazy val helloWorld = BuildDefinition
  .sparkApp("hello-world")
  .dependsOn(sparkUtils)
