
lazy val sparkTools = RootProject(file("spark-tools"))


lazy val traceability =
  project
    .in(file("."))
    .aggregate(sparkTools)
