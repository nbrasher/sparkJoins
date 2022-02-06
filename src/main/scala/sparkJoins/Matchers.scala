package sparkJoins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import com.typesafe.scalalogging.LazyLogging

abstract class BaseMatcher extends LazyLogging {
    def run (left: DataFrame, right: DataFrame): DataFrame
}

class RDDMatcher extends BaseMatcher {
    def run (left: DataFrame, right: DataFrame): DataFrame = {
        return left.join(right, usingColumns=Seq("name", "domain"))
    }
}

class FrameMatcher extends BaseMatcher {
    def run (left: DataFrame, right: DataFrame): DataFrame = {
        return left
    }
}
