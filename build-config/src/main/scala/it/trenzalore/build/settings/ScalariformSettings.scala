package it.trenzalore.build.settings

import com.typesafe.sbt.SbtScalariform.autoImport.scalariformPreferences
import scalariform.formatter.preferences._

object ScalariformSettings {

  /**
    * Configuration of Scalariform plugin (source code formatting)
    *
    * url : https://github.com/sbt/sbt-scalariform
    */
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

}