package sparkJoins

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers._

class MatchersSpec extends AnyFunSpec with TestingSparkSession {
    // Import needs to be in a spark context, hence why its here
    import spark.sqlContext.implicits._

    val left = Seq(
        (1, "Tesla", "tesla.com"),
        (2, "SpaceX", "spacex.com"),
        (3, "nonmatch", "nothing.com"),
        (7, "Close", "close.com")
    ).toDF("id1", "name", "domain")

    val right = Seq(
        (4, "Tesla", "tesla.com"),
        (5, "SpaceX", "spacex.com"),
        (6, "Nonsense", "nonsense.com"),
        (8, "closer", "closer.com")
    ).toDF("id2", "name", "domain")

    val rddm = new RDDMatcher()
    val dfm = new FrameMatcher()

    describe("test RDD matcher") {
        val res = rddm.run(left, right)

        // The .as[Int] makes these Set[Int] rather than Set[spark.sql.Row]
        val leftIds = res.select("id1").as[Int].collect().toSet
        val rightIds = res.select("id2").as[Int].collect().toSet

        it ("should only have matches remaining") {
            assert(res.count() === 2)
            val expectedLeft = Set(1, 2)
            val expectedRight = Set(4, 5)

            leftIds.foreach { lid => assert(expectedLeft.contains(lid)) }
            rightIds.foreach { rid => assert( expectedRight.contains(rid)) }
        }
    }

    describe("test dataframe macher") {
        val res = dfm.run(left, right)

        // The .as[Int] makes these Set[Int] rather than Set[spark.sql.Row]
        val leftIds = res.select("id1").as[Int].collect().toSet
        val rightIds = res.select("id2").as[Int].collect().toSet

        it ("should only have matches remaining") {
            assert(res.count() === 3)
            val expectedLeft = Set(1, 2, 7)
            val expectedRight = Set(4, 5, 8)

            leftIds.foreach { lid => assert(expectedLeft.contains(lid)) }
            rightIds.foreach { rid => assert( expectedRight.contains(rid)) }
        }        
    }
}
