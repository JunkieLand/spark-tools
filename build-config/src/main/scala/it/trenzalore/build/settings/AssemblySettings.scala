package it.trenzalore.build.settings

import sbt.Keys._
import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.PathList

object AssemblySettings {

  /**
    * Configuration of Assembly Plugin : generate a fat jar, meaning a jar with all the librairies inside
    *
    * url : https://github.com/sbt/sbt-assembly
    *
    * command :
    * - sbt assembly
    */
  lazy val assemblySettings = Seq(
    test in assembly := {}, // Do not play test when building assembly
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(cacheOutput = false), // Do not check SHA-1 of each jar/class
    assemblyMergeStrategy in assembly := {
      case x if Assembly.isConfigFile(x)           ⇒ MergeStrategy.concat
      case "META-INF/io.netty.versions.properties" ⇒ MergeStrategy.first
      case PathList("org", _, xs @ _*)             ⇒ MergeStrategy.last
      case PathList("com", _, xs @ _*)             ⇒ MergeStrategy.last
      case PathList("javax", "inject", xs @ _*)    ⇒ MergeStrategy.last
      case PathList("overview.html")               ⇒ MergeStrategy.last
      case "application.conf"                      ⇒ MergeStrategy.concat
      case "log4j.properties"                      ⇒ MergeStrategy.last
      case x ⇒
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )

}