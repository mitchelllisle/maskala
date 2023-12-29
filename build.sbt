ThisBuild / version := "0.8.0"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "maskala",
    publishTo := Some("GitHub Maven Packages Repo" at "https://maven.pkg.github.com/mitchelllisle/maskala"),
    credentials += Credentials(
      "GitHub Package Registry", "maven.pkg.github.com", System.getenv("USERNAME"), System.getenv("TOKEN")
    )

  )

val sparkVersion = "3.5.0"
val circeVersion = "0.14.6"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "io.circe" %% "circe-generic" % circeVersion,
  "com.amazonaws.secretsmanager" % "aws-secretsmanager-caching-java" % "1.0.2",
  "io.circe" %% "circe-yaml" % "1.15.0",
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "com.swoop" %% "spark-alchemy" % "1.2.1"
)

dependencyOverrides += "org.scala-lang.modules" %% "scala-xml" % "2.1.0"
