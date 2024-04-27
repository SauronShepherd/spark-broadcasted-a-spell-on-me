name := "spark-broadcasted-a-spell-on-me"
version := "1.0"
scalaVersion := "2.11.12"

// Dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.7",
  "org.apache.spark" %% "spark-sql" % "2.4.7"
)
