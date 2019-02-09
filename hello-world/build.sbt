import it.trenzalore.build.Dependencies
import it.trenzalore.build.BuildDefinition

lazy val commons = RootProject(file("../commons"))

lazy val helloWorld = BuildDefinition
  .sparkApp("hello-world")
  .dependsOn(commons)
