package sparkJoins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{ col, udf, explode, broadcast }
import org.apache.spark.sql.{ DataFrame, SparkSession }

import com.typesafe.scalalogging.LazyLogging

abstract class BaseMatcher extends LazyLogging {
    val tldUdf = udf(MatchingUtils.tldPrefix(_))
    val blockUdf = udf(MatchingUtils.genBlockingKey(_, 4, 5))

    def block (df: DataFrame): DataFrame = {
        val blockedDF = df.withColumn("blockingKeys", blockUdf(tldUdf(col("domain"))))
        return blockedDF.select(
            col("id"),
            col("name"),
            col("domain"),
            explode(col("blockingKeys")).alias("blockingKey")
        )
    }
}

class FrameMatcher extends BaseMatcher {    
    def run (left: DataFrame, right: DataFrame): DataFrame = {
        val leftBlocked = block(left).select(
            col("id").alias("id1"), 
            col("name").alias("name1"),
            col("domain").alias("domain1"),
            col("blockingKey")
        )
        leftBlocked.cache()

        val rightBlocked = block(right).select(
            col("id").alias("id2"), 
            col("name").alias("name2"),
            col("domain").alias("domain2"),
            col("blockingKey")
        )
        rightBlocked.cache()

        val joined = leftBlocked.join(rightBlocked, usingColumn="blockingKey")

        val ret = joined.dropDuplicates(Seq("id1", "id2"))

        return ret
    }
}

class BroadcastFrameMatcher extends BaseMatcher {    
    def run (left: DataFrame, right: DataFrame): DataFrame = {
        val leftBlocked = block(left).select(
            col("id").alias("id1"), 
            col("name").alias("name1"),
            col("domain").alias("domain1"),
            col("blockingKey")
        )
        leftBlocked.cache()

        val rightBlocked = block(right).select(
            col("id").alias("id2"), 
            col("name").alias("name2"),
            col("domain").alias("domain2"),
            col("blockingKey")
        )
        rightBlocked.cache()

        val joined = leftBlocked.join(broadcast(rightBlocked), usingColumn="blockingKey")

        val ret = joined.dropDuplicates(Seq("id1", "id2"))

        return ret
    }
}
