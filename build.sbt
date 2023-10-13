ThisBuild / version := "0.3.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "maskala"
  )

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,
  "org.apache.spark" %% "spark-sql" % "3.3.2" % "provided",
  "org.apache.spark" %% "spark-core" % "3.3.2" % "provided",
)
