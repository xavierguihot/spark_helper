name := "spark_helper"

version := "2.0.1"

scalaVersion := "2.11.12"

scalacOptions ++= Seq(
  "-unchecked",
  "-deprecation",
  "-feature",
  "-Ywarn-unused-import",
  "-Ywarn-unused"
)

assemblyJarName in assembly := name.value + "-" + version.value + ".jar"

assemblyOutputPath in assembly := file(
  "./" + name.value + "-" + version.value + ".jar")

testOptions in Test += Tests.Argument("-oD")
parallelExecution in Test := false

wartremoverWarnings in (Compile, compile) ++= Warts.all
wartremoverWarnings in (Compile, compile) --= Seq(
  Wart.DefaultArguments,
  Wart.Nothing,
  Wart.Equals,
  Wart.NonUnitStatements,
  Wart.Overloading,
  Wart.Option2Iterable
)

scalafmtOnCompile := true

val sparkVersion = "2.1.0"
val apacheCommonVersion = "3.5"
val typesafeVersion = "1.3.1"
val jodaTimeVersion = "2.9.9"
val jodaConvertVersion = "1.9.2"
val scalaTestVersion = "3.0.4"
val sparkTestVersion = "2.1.0_0.8.0"

libraryDependencies ++= Seq(
  "org.apache.spark"   %% "spark-core"         % sparkVersion % "provided",
  "org.apache.commons" % "commons-lang3"       % apacheCommonVersion,
  "com.typesafe"       % "config"              % typesafeVersion,
  "joda-time"          % "joda-time"           % jodaTimeVersion,
  "org.joda"           % "joda-convert"        % jodaConvertVersion,
  "org.scalatest"      %% "scalatest"          % scalaTestVersion % "test",
  "com.holdenkarau"    %% "spark-testing-base" % sparkTestVersion % "test"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x                             => MergeStrategy.first
}
