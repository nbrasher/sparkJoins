package sparkJoins

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.scalatest._

trait TestingSparkSession extends BeforeAndAfterAll {
  self: Suite =>

  lazy val spark: SparkSession = TestingSparkSession.sparkSingleton
  lazy val sc: SparkContext = spark.sparkContext
  lazy val sqlContext: SQLContext = spark.sqlContext
}

object TestingSparkSession {
  lazy val sparkSingleton: SparkSession = {
    SparkSession
      .builder()
      .config("spark.executor.memory", "512mb")
      .config("spark.ui.showConsoleProgress", value = false)
      .master("local[4]")
      .getOrCreate()
  }
}
