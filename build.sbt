ThisBuild / version := "0.5.1-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "maskala",
    publishTo := Some("GitHub Maven Packages Repo" at "https://maven.pkg.github.com/mitchelllisle/maskala"),
    credentials += Credentials(
      "GitHub Package Registry", "maven.pkg.github.com", System.getenv("USERNAME"), System.getenv("TOKEN")
    )

  )

val sparkVersion = "3.3.2"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "com.swoop" %% "spark-alchemy" % "1.2.1"
)

dependencyOverrides += "org.scala-lang.modules" %% "scala-xml" % "1.2.0"

