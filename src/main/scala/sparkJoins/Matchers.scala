package sparkJoins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{ col, udf, explode }
import org.apache.spark.sql.{DataFrame, SparkSession}

import com.typesafe.scalalogging.LazyLogging

trait BaseMatcher extends LazyLogging {
    def run (left: DataFrame, right: DataFrame): DataFrame
}

class RDDMatcher extends BaseMatcher {
    def run (left: DataFrame, right: DataFrame): DataFrame = {
        return left.join(right, usingColumns=Seq("name", "domain"))
    }
}

class FrameMatcher extends BaseMatcher {
    val blockUdf = udf(MatchingUtils.genSimple(_))
    def run (left: DataFrame, right: DataFrame): DataFrame = {
        val leftBlocked = left.withColumn("blockingKeys", blockUdf(col("domain"))).select(
            col("id1"), col("name"), col("domain"), explode(col("blockingKeys"))
        )
        leftBlocked.cache()

        val rightBlocked = right.withColumn("blockingKeys", blockUdf(col("domain"))).select(
            col("id2"), col("name"), col("domain"), explode(col("blockingKeys"))
        )
        rightBlocked.cache()

        val joined = leftBlocked.join(rightBlocked, usingColumn="col")

        val ret = joined.dropDuplicates(Seq("id1", "id2"))

        return ret
    }
}
