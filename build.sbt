organization := "com.ebiznext.flume"

name := "flume-elasticsearch-serializers"

version := "0.1-SNAPSHOT"

crossScalaVersions in ThisBuild := Seq("2.10.6", "2.11.8")

//scalaVersion := "2.11.8"

val flumeV = "1.6.0"

val elasticSearchV = "1.7.3"

libraryDependencies ++= Seq(
  "org.elasticsearch" % "elasticsearch" % elasticSearchV % Provided,
  "org.apache.flume" % "flume-ng-core" % flumeV % Provided,
  "org.apache.flume" % "flume-ng-sdk" % flumeV % Provided,
  "org.apache.flume.flume-ng-sinks" % "flume-ng-elasticsearch-sink" % flumeV % Provided,
  "com.typesafe" % "config" % "1.2.1" % Test,
  "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2" % Test,
  "junit" % "junit" % "4.12" % Test,
  "com.novocode" % "junit-interface" % "0.11" % Test
)

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

scalacOptions += "-target:jvm-1.7"

resolvers += "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository"

resolvers += "ebiz repo" at "http://art.ebiznext.com/artifactory/libs-release-local"

resolvers += "ebiz snaphost" at "http://art.ebiznext.com/artifactory/libs-snapshot-local"

resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/"

publishTo := {
  val repo = "http://art.ebiznext.com/artifactory/"
  if (version.value.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at repo + "libs-snapshot-local")
  else
    Some("releases" at repo + "libs-release-local")
}

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

publishArtifact in(Compile, packageSrc) := true

publishArtifact in(Test, packageSrc) := true

fork := true
