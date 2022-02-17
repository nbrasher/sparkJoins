package sparkJoins

object MatchingUtils {
    def tldPrefix(s: String): Option[String] = {
        if (s == null) {
            None
        } else {
            s.strip().toLowerCase().split('.').head match {
                case "" => None
                case s => Some(s)
            }
        }
    }

    def genBlockingKey(s: Option[String], numTake: Int = 5, window: Int = 4): Seq[String] = {
        s match {
            case None => Seq()
            case Some(s) => s.sliding(window).take(numTake).toSeq
        }
    }

    def genSimple(s: String) = { s.sliding(4).take(5).toSeq }
}