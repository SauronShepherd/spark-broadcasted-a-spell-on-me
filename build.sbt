name := "spark-broadcasted-a-spell-on-me"
version := "1.0"

// Spark 2.4.7
scalaVersion := "2.11.12"
val sparkVersion = "2.4.7"

// Spark 3.5.0
//scalaVersion := "2.12.17"
//val sparkVersion = "3.5.0"


// Dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion
)
