import org.apache.spark.sql.{Row, SparkSession}

import scala.io.StdIn

object ThirdTry {


  def main(args: Array[String]): Unit = {
    // 1. Configure Spark
    val spark = SparkSession.builder
                            .master("local")
                            .getOrCreate()

    // 2. Load the big table dataframe and save it using parquet format with snappy compression
    spark.read
         .option("header","true")
         .csv("src/main/resources/people-500000.csv")
         .write
         .format("parquet")
         .mode("overwrite")
         .option("compress","snappy")
         .save("src/main/resources/big_table")
    val bigDF = spark.read
                     .load("src/main/resources/big_table")

    // 3. Increase the threshold for autobroadcast
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", bigDF.queryExecution.analyzed.stats.sizeInBytes.toLong)

    // 4. Load the small table dataframe
    val smallDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], bigDF.schema)

    // 5. Join and count by index
    val joinByIndexDF = bigDF.join(smallDF, "index")
    joinByIndexDF.count()

    // 6. Join and count by all fields
    try {
      val byAllColumns = bigDF.columns.toSet.intersect(smallDF.columns.toSet).toList
      val joinByAllDF = bigDF.join(smallDF, byAllColumns)
      joinByAllDF.count()
    } catch {
      case ex: OutOfMemoryError => ex.printStackTrace()
    }

    // 7. Print sizes, readLine
    // and stop
    println(s"bigDF sizeInBytes: ${bigDF.queryExecution.analyzed.stats.sizeInBytes}")
    println(s"smallDF sizeInBytes: ${smallDF.queryExecution.analyzed.stats.sizeInBytes}")
    StdIn.readLine("Press enter to finish ...")
    spark.stop()
  }
}
