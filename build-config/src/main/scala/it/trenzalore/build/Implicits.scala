package it.trenzalore.build

import com.typesafe.sbt.GitPlugin.autoImport._
import com.typesafe.sbt.GitVersioning
import it.trenzalore.build.settings.{ AssemblySettings, ProjectSettings, PublishSettings }
import sbt.Keys._
import sbt.Project
import sbtbuildinfo.BuildInfoPlugin
import sbtbuildinfo.BuildInfoPlugin.autoImport._

object Implicits {

  implicit class ProjectPublishable(project: Project) {
    def publishAssembly(): Project = {
      project
        .enablePlugins(sbtassembly.AssemblyPlugin, GitVersioning, BuildInfoPlugin)
        .settings(PublishSettings.publishSettings: _*)
        .settings(AssemblySettings.assemblySettings: _*)
        .settings(
          buildInfoKeys := Seq[BuildInfoKey](name, organization, version, scalaVersion, sbtVersion, git.gitHeadCommit),
          buildInfoPackage := ProjectSettings.basePackages
        )
    }

    def isNotPublishable(): Project = {
      project
        .disablePlugins(sbtassembly.AssemblyPlugin, GitVersioning, BuildInfoPlugin)
        .settings(publish := {})
    }
  }

  implicit class BooleanMap[A](a: A) {
    def mapCondition(condition: Boolean, f: A ⇒ A): A = {
      if (condition)
        f(a)
      else
        a
    }
  }

}