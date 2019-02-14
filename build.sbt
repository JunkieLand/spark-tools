
lazy val sparkUtils = RootProject(file("spark-utils"))
lazy val sparkSourceManager = RootProject(file("spark-source-manager"))


lazy val sparkTools =
  project
    .in(file("."))
    .aggregate(sparkUtils, sparkSourceManager)
