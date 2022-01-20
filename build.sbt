name := "sparkJoins"
 
version := "0.1"

scalaVersion := "2.12.13"

val sparkVersion = "3.1.1"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % "2.13.4",
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.scalatest" %% "scalatest" % "3.2.10" % "test",
  "org.scalatest" %% "scalatest-funspec" % "3.2.10" % "test",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
  "org.slf4j" % "slf4j-log4j12" % "1.7.33",
  "org.apache.logging.log4j" % "log4j-api" % "2.17.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.17.1",
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

mainClass in assembly := Some("sparkJoins.Main")
