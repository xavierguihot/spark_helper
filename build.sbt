name := "spark_helper"

version := "1.0.11"

scalaVersion := "2.11.8"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-Xfatal-warnings")

assemblyJarName in assembly := name.value + "-" + version.value + ".jar"

assemblyOutputPath in assembly := file("./" + name.value + "-" + version.value + ".jar")

libraryDependencies += "joda-time" % "joda-time" % "2.9.9"

libraryDependencies += "org.joda" % "joda-convert" % "1.9.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0" % "provided"

libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.5"

libraryDependencies += "com.typesafe" % "config" % "1.3.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.1.0_0.8.0" % "test"

parallelExecution in Test := false

assemblyMergeStrategy in assembly := {
	case PathList("META-INF", xs @ _*) => MergeStrategy.discard
	case x => MergeStrategy.first
}

scalacOptions in (Compile, doc) ++= Seq(
	"-no-link-warnings" // Suppresses problems with Scaladoc @throws links
)
