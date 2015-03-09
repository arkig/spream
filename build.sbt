name := "spream"

version := "0.1"

organization := "arkig"

resolvers += "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"

resolvers += "mvnrepository" at "http://mvnrepository.com/artifact/"

resolvers += "commbank-releases" at "http://commbank.artifactoryonline.com/commbank/ext-releases-local"

resolvers ++= Seq(
  //"Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
)

libraryDependencies ++= Seq(
  "org.scalaz"             %% "scalaz-core"   % "7.0.6",
  "org.scalaz.stream"      %% "scalaz-stream" % "0.5",
  "org.specs2"             %% "specs2"        % "2.3.12" % "test",
  "org.scalatest"          %  "scalatest_2.10" % "2.2.4" % "test",
  "com.github.nscala-time" %% "nscala-time"   % "1.2.0"
)

libraryDependencies ++= Seq(
  "au.com.cba.omnia" %% "piped"             % "1.8.0-20140621112319-54875e4" intransitive(),
  "au.com.cba.omnia" %% "permafrost"        % "0.2.0-20150113073328-8994d5b" intransitive(),
  "au.com.cba.omnia" %% "omnitool-core"     % "1.5.0-20150113041805-fef6da5" intransitive()
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.2.0" % "provided",
  "org.apache.hadoop" % "hadoop-client" % "2.4.1" % "provided"
)

scalaVersion := "2.10.4"

scalacOptions ++= Seq("-deprecation", "-unchecked", "-Xlint")
