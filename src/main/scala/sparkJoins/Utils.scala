package sparkJoins

object MatchingUtils {
    def tldPrefix(s: String): Option[String] = {
        if (s == null) {
            None
        } else {
            s.strip().toLowerCase().split('.').headOption
        }
    }

    def genBlockingKey(s: Option[String], numTake: Int = 5, window: Int = 4): Seq[String] = {
        s match {
            case None => Seq()
            case Some(s) => s.sliding(window).take(numTake).toSeq
        }
    }
}