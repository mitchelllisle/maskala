ThisBuild / version := "1.3.0"

ThisBuild / scalaVersion := "2.12.20"

lazy val root = (project in file("."))
  .settings(
    name := "maskala",
    publishTo := Some("GitHub Maven Packages Repo" at "https://maven.pkg.github.com/mitchelllisle/maskala"),
    credentials += Credentials(
      "GitHub Package Registry", "maven.pkg.github.com", System.getenv("GH_USERNAME"), System.getenv("GH_TOKEN")
    )

  )

val sparkVersion = "3.5.6"
val circeVersion = "0.14.14"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.19" % Test,
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-yaml" % "1.15.0",
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "com.swoop" %% "spark-alchemy" % "1.2.1"
)

dependencyOverrides += "org.scala-lang.modules" %% "scala-xml" % "2.4.0"
