import org.apache.spark.sql.SparkSession

import scala.io.StdIn

object SecondTry {

  def main(args: Array[String]): Unit = {

    // 1. Configure Spark
    val spark = SparkSession.builder
                            .master("local")
                            .getOrCreate()

    // 2. Load the big table dataframe
    val bigDF = spark.read
                     .option("header","true")
                     .csv("src/main/resources/people-500000.csv")

    // 3. Load the small table dataframe
    val smallDF = spark.read
                       .option("header","true")
                       .csv("src/main/resources/people-0.csv")

    // 4. Join and count
    val joinDF = bigDF.join(smallDF, "index")
    joinDF.count()

    // 6. Print sizes, readLine and stop
    println(s"bigDF sizeInBytes: ${bigDF.queryExecution.analyzed.stats.sizeInBytes}")
    println(s"smallDF sizeInBytes: ${smallDF.queryExecution.analyzed.stats.sizeInBytes}")
    StdIn.readLine("Press enter to finish ...")
    spark.stop()
  }
}
