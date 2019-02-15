import com.typesafe.sbt.SbtScalariform.autoImport.scalariformPreferences
import scalariform.formatter.preferences._
import sbtrelease.ReleasePlugin.autoImport._
import sbtrelease.ReleasePlugin.runtimeVersion
import sbtrelease.ReleaseStateTransformations._
import sbt.Keys._


lazy val sparkTools = Project("spark-tools", new File("."))
  .settings(
    organization := "it.trenzalore",
    scalaVersion := Versions.scalaVersion,
    sbtVersion := Versions.sbtVersion,
    crossScalaVersions := Seq(scalaVersion.value),
    logLevel := Level.Info,
    parallelExecution := false,
    offline := true,
    fork := true,
    javaOptions += "-Dsbt.override.build.repos=true -Xmx2G",
    scalacOptions ++= Seq("-feature", "-unchecked", "-deprecation")
  )
  .settings(scalariformPluginSettings: _*)
  .settings(releaseSettings: _*)
  .settings(
    resolvers ++= Dependencies.resolvers,
    coursierResolvers ++= Dependencies.resolvers,
    libraryDependencies ++= Dependencies.commonLibraries
  )
  .settings(
    libraryDependencies ++= Dependencies.sparkDependencies(isProvided = true)
  )
  .settings(
    libraryDependencies ++= Vector(
      Dependencies.typesafeConfig,
      Dependencies.scalaLogging,
      Dependencies.enumeratum,
      Dependencies.ficus
    )
  )


lazy val scalariformPluginSettings = Seq(
  scalariformPreferences := scalariformPreferences.value
    .setPreference(AlignArguments, false)
    .setPreference(AlignParameters, true)
    .setPreference(AlignSingleLineCaseStatements, true)
    .setPreference(AllowParamGroupsOnNewlines, false)
    .setPreference(CompactControlReadability, false)
    .setPreference(CompactStringConcatenation, false)
    .setPreference(DanglingCloseParenthesis, Force)
    .setPreference(DoubleIndentConstructorArguments, false)
    .setPreference(DoubleIndentMethodDeclaration, false)
    .setPreference(FirstArgumentOnNewline, Force)
    .setPreference(FirstParameterOnNewline, Force)
    .setPreference(FormatXml, true)
    .setPreference(IndentLocalDefs, false)
    .setPreference(IndentPackageBlocks, true)
    .setPreference(IndentSpaces, 2)
    .setPreference(IndentWithTabs, false)
    .setPreference(MultilineScaladocCommentsStartOnFirstLine, false)
    .setPreference(NewlineAtEndOfFile, false)
    .setPreference(PlaceScaladocAsterisksBeneathSecondAsterisk, true)
    .setPreference(PreserveSpaceBeforeArguments, false)
    .setPreference(RewriteArrowSymbols, true)
    .setPreference(SingleCasePatternOnNewline, true)
    .setPreference(SpaceBeforeColon, false)
    .setPreference(SpaceBeforeContextColon, false)
    .setPreference(SpaceInsideBrackets, false)
    .setPreference(SpaceInsideParentheses, false)
    .setPreference(SpacesAroundMultiImports, true)
    .setPreference(SpacesWithinPatternBinders, true)
)


lazy val releaseSettings = Seq(
  releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    runClean,
    runTest,
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
    publishArtifacts,
    setNextVersion,
    commitNextVersion,
    pushChanges
  ),
  releaseTagName := s"v${runtimeVersion.value}-${name.value.toUpperCase().replace("-", "_")}",
  releaseCommitMessage := s"Setting version to ${runtimeVersion.value} for module ${name.value}"
)