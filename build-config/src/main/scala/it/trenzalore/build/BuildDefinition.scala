package it.trenzalore.build

import coursier.CoursierPlugin.autoImport.coursierResolvers
import it.trenzalore.build.settings.{ ProjectSettings, ReleaseSettings, ScalariformSettings }
import sbt.Keys._
import sbt._

object BuildDefinition {

  import Implicits._

  def sparkApp(name: String, publication: Publication = Assembly, isSparkProvided: Boolean = true) = {
    basicApp(name, publication).settings(
      libraryDependencies ++= Dependencies.sparkDependencies(isSparkProvided)
    )
  }

  def basicApp(name: String, publication: Publication = Assembly) = {
    library(name, publication).settings(
      libraryDependencies ++= Dependencies.appDedendencies
    )
  }

  def library(name: String, publication: Publication = Assembly) = {
    Project(name, new File("."))
      .settings(
        organization := ProjectSettings.organization,
        scalaVersion := Versions.scalaVersion,
        sbtVersion := Versions.sbtVersion,
        crossScalaVersions := Seq(scalaVersion.value),
        logLevel := Level.Info,
        parallelExecution := false,
        offline := true,
        fork := true,
        javaOptions += "-Dsbt.override.build.repos=true -Xmx8G",
        scalacOptions ++= Seq(
          "-feature",
          "-unchecked",
          "-deprecation"
        ),
        mappings in (Compile, packageBin) ~= ignoreFiles("/data/")
      )
      .settings(ScalariformSettings.scalariformPluginSettings: _*)
      .settings(ReleaseSettings.releaseSettings: _*)
      .settings(
        resolvers ++= Dependencies.resolvers,
        coursierResolvers ++= Dependencies.resolvers,
        libraryDependencies ++= Dependencies.commonLibraries
      )
      .mapCondition(publication == None, project ⇒ project.isNotPublishable())
      .mapCondition(publication == Assembly, project ⇒ project.publishAssembly())
  }

  private def ignoreFiles(ignoredFiles: String*): (Seq[(File, String)]) ⇒ Seq[(File, String)] = { mapping ⇒
    mapping.filter {
      case (file, map) ⇒
        ignoredFiles.forall(ignoredFile ⇒ !file.getAbsolutePath.contains(ignoredFile))
    }
  }

}
