
lazy val buildConfig = file("../../build-config")

lazy val root = project
  .in(file("."))
  .dependsOn(RootProject(buildConfig))
