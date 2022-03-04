package sparkJoins

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import com.typesafe.scalalogging.LazyLogging

object Main extends LazyLogging{

  def main(args: Array[String]): Unit = {
    logger.info("Spark joins testing")
    val spark = SparkSession.builder
      .appName("sparkJoins")
      .getOrCreate()
    
    val df = spark.read.format("csv")
      .option("header", "true")
      .load("training_data.csv")

    df.printSchema(10)
    logger.info(s"Total Dataframe dimensions: ${df.count()} x ${df.columns.length}")

    // Split data frame
    val left = df.select(
      col("id1").as("id"),
      col("name1").as("name"),
      col("domain1").as("domain"), 
    )
    val right = df.select(
      col("id2").as("id"),
      col("name2").as("name"),
      col("domain2").as("domain"), 
    )

    // Run one matcher, currently for show
    logger.info("Running one matcher")
    val rddm = new FrameMatcher()
    val ret = rddm.run(left, right)

    ret.show(10)

    logger.info("Finished!")
  }
}
