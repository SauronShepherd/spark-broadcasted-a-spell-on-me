import org.apache.spark.sql.{Row, SparkSession}

import scala.io.StdIn
import scala.util.Random

object FirstTry {

  def main(args: Array[String]): Unit = {

    // 1. Configure Spark
    val spark = SparkSession.builder
                            .master("local")
                            .getOrCreate()

    import spark.implicits._

    // 2. Define the function to generate random records
    def generateRandomRecord(seed: Long): (Int, Double) = {
      val rand = new Random(seed)
      (rand.nextInt(), rand.nextDouble())
    }
    // 3. Create the big table dataframe
    val numRows = 3000000
    val bigRDD = spark.sparkContext.parallelize(1 to numRows, numSlices = 2)
      .map(seed => generateRandomRecord(seed))
    val bigDF = bigRDD.toDF("key", "value")

    // 4. Create the small table dataframe
    val smallDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], bigDF.schema)

    // 5. Join and count
    val joinDF = bigDF.join(smallDF, "key")
    joinDF.count()

    // 6. Print sizes, readLine and stop
    println(s"bigDF sizeInBytes: ${bigDF.queryExecution.analyzed.stats.sizeInBytes}")
    println(s"smallDF sizeInBytes: ${smallDF.queryExecution.analyzed.stats.sizeInBytes}")
    StdIn.readLine("Press enter to finish ...")
    spark.stop()
  }
}
