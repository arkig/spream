name := "spream"

version := "0.1.1-SNAPSHOT"

organization := "com.arkig"

resolvers += "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"

resolvers += "mvnrepository" at "http://mvnrepository.com/artifact/"

resolvers ++= Seq(
  //"Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
)

libraryDependencies ++= Seq(
  "org.scalaz"             %% "scalaz-core"   % "7.0.6",
  "org.scalaz.stream"      %% "scalaz-stream" % "0.5",
  "org.specs2"             %% "specs2"        % "2.3.12" % "test",
  "org.scalatest"          %  "scalatest_2.10" % "2.2.4" % "test"
)

//"org.apache.spark" %% "spark-core" % "1.2.0" % "provided",
//"org.apache.hadoop" % "hadoop-client" % "2.4.1" % "provided"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.0" % "provided",
  "org.apache.hadoop" % "hadoop-client" % "2.6.0" % "provided"
)

scalaVersion := "2.10.4"

scalacOptions ++= Seq("-deprecation", "-unchecked", "-Xlint")

// Overkill. Necessary to prevent Spark test suits running in parallel (causes issues).
// All specs are fine to run in parallel.
parallelExecution in Test := false
