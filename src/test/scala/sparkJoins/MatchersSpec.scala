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
        (4, "Close", "close.com"),
        (5, "Patch", "patch.io"),
    ).toDF("id", "name", "domain")

    val right = Seq(
        (6, "Tesla", "tesla.com"),
        (7, "SpaceX", "spacex.com"),
        (8, "Nonsense", "nonsense.com"),
        (9, "closer", "closer.com"),
        (10, "Patch", "getpatch.com"),
    ).toDF("id", "name", "domain")

    val dfm = new FrameMatcher()
    val bdfm = new BroadcastFrameMatcher()

    describe("test dataframe macher") {
        val res = dfm.run(left, right)

        // The .as[Int] makes these Set[Int] rather than Set[spark.sql.Row]
        val leftIds = res.select("id1").as[Int].collect().toSet
        val rightIds = res.select("id2").as[Int].collect().toSet

        it ("should only have matches remaining") {
            assert(res.count() === 4)
            val expectedLeft = Set(1, 2, 4, 5)
            val expectedRight = Set(6, 7, 9, 10)

            leftIds.foreach { lid => assert(expectedLeft.contains(lid)) }
            rightIds.foreach { rid => assert( expectedRight.contains(rid)) }
        }        
    }

    describe("test dataframe macher with a broadcast join") {
        val res = bdfm.run(left, right)

        val leftIds = res.select("id1").as[Int].collect().toSet
        val rightIds = res.select("id2").as[Int].collect().toSet

        it ("should only have matches remaining") {
            assert(res.count() === 4)
            val expectedLeft = Set(1, 2, 4, 5)
            val expectedRight = Set(6, 7, 9, 10)

            leftIds.foreach { lid => assert(expectedLeft.contains(lid)) }
            rightIds.foreach { rid => assert( expectedRight.contains(rid)) }
        }        
    }
}
