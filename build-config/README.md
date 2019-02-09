# TRC-Build

This project gathers the build configuration of every traceability project.

## Application templates

Some predefined application templates are available :

### Library

It's the most simple kind of project. It has all the settings required to
develop, test, release, publish... but has the minimum possible dependencies.

    // build.sbt

    import com.renault.hercules.traceability.build.BuildDefinition

    lazy val myLibrary = BuildDefinition
      .library("myLibrary", isPublishable = false)

### Basic application

It extends the `library`. It adds dependencies required for an "app" to work
such as logging, argument parsing, configuration.

    // build.sbt

    import com.renault.hercules.traceability.build.BuildDefinition

    lazy val myBasicApp = BuildDefinition
      .basicApp("myBasicApp", isPublishable = false)

### Spark application

It extends the `basic app` by adding Spark dependencies.

    // build.sbt

    import com.renault.hercules.traceability.build.BuildDefinition

    lazy val mySparkApp = BuildDefinition
      .sparkApp("mySparkApp", isPublishable = false)

## Dependencies

All re-usable dependencies are set in the file `Dependencies.scala`.

## Settings

### Release

Release settings are set in the file `settings/ReleaseSettings.scala`. It uses
the default release process provided by the `sbt-release` plugin. Note that
tagging the release has been disabled since we have multiple independent
project inside a single Git repository, and Git tags have to be unique.

In order to release :

    sbt "release with-defaults next-version 9.9.9-SNAPSHOT"

If you want a release version different than the one coming from `version.sbt` :

    sbt "release with-defaults release-version 1.0.0 next-version 9.9.9-SNAPSHOT"

### Publish

Publish settings are set in the file `settings/PublishSettings.scala`.

The Nexus repository used to publish our artifact is Renault Digital' Nexus :

    https://repo.renault-digital.com/repository/

To pull or push from this repository, be sure to have the required permissions,
and set your credentials in the file `~/.sbt/.credentials` :

    # ~/.sbt/.credentials

    realm=Sonatype Nexus Repository Manager
    host=repo.renault-digital.com
    user=ID_RENAULT_DIGITAL
    password=MOT_DE_PASSE_RENAULT_DIGITAL

### Scalariform

The code formatting plugin `Scalariform` is used. It will format the project code at compile time.
The formatting rules are set in the file `settings/ScalariformSettings.scala`.