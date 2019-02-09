package it.trenzalore.build.settings

import sbtrelease.ReleasePlugin.autoImport._
import sbtrelease.ReleasePlugin.runtimeVersion
import sbtrelease.ReleaseStateTransformations._
import sbt.Keys._

object ReleaseSettings {

  // Skip Git tag to avoid conflict when a project is set to a version already used
  // by another project in the repository.
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

}
