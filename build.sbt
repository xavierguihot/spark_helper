name := "spark_helper"

version := "1.0.0"

scalaVersion := "2.10.4"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xfatal-warnings")

assemblyJarName in assembly := name.value + "-" + version.value + ".jar"

assemblyOutputPath in assembly := file("./" + name.value + "-" + version.value + ".jar")

libraryDependencies += "org.scalatest" %% "scalatest" % "1.9.1" % "test"

libraryDependencies += "joda-time" % "joda-time" % "2.9.4"

libraryDependencies += "org.joda" % "joda-convert" % "1.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1" % "provided"

libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.5"

parallelExecution in Test := false

assemblyMergeStrategy in assembly := {
	case PathList("META-INF", xs @ _*) => MergeStrategy.discard
	case x => MergeStrategy.first
}
