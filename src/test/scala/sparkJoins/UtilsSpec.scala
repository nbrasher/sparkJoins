package sparkJoins

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers._

class UtilSpec extends AnyFunSpec {
    describe("tldPrefix") {
        it ("should handle whitespace and uppercase") {
            assert(MatchingUtils.tldPrefix("   Google.com ") == Some("google"))
        }
    }
    describe("genBlockingKeys") {
        it ("Should generate blocking keys of length 4") {
            MatchingUtils.genBlockingKey(Some("thisisanaddress")) should equal (
                Seq("this", "hisi", "isis", "sisa", "isan")
            )
        }
        it ("Should handle shorter strings") {
            MatchingUtils.genBlockingKey(Some("google")) should equal (Seq("goog", "oogl", "ogle"))
        }
        it ("Should handle None") {
            MatchingUtils.genBlockingKey(None) should equal (Seq())
        }
    }
}