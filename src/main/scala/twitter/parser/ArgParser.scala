package twitter.parser

import scala.util.Try

object ArgParser {
  val DEFAULT_UNSET = "unset"

  case class CliArgs(
    outputTopic: String = "twitter_tweets",
    consumerKey: String = DEFAULT_UNSET,
    consumerSecret: String = DEFAULT_UNSET,
    token: String = DEFAULT_UNSET,
    secret: String = DEFAULT_UNSET
  )

  private val parser = new scopt.OptionParser[CliArgs]("scopt") {

    opt[String]("outputTopic")
      .text("The topic to produce tweets to")
      .required()
      .action { (x, c) =>
        c.copy(outputTopic = x)
      }

    opt[String]("consumerKey")
      .text("twitter consumerKey")
      .required()
      .action { (x, c) =>
        c.copy(consumerKey = x)
      }

    opt[String]("consumerSecret")
      .text("twitter consumerSecret")
      .required()
      .action { (x, c) =>
        c.copy(consumerSecret = x)
      }

    opt[String]("token")
      .text("twitter token")
      .required()
      .action { (x, c) =>
        c.copy(token = x)
      }

    opt[String]("secret")
      .text("twitter secret")
      .required()
      .action { (x, c) =>
        c.copy(secret = x)
      }

  }

  def parse(args: Array[String]): Try[ArgParser.CliArgs] =
    parser
      .parse(args, CliArgs())
      .toRight(new IllegalArgumentException("Arg parser unable to initialize! Fatal Error")).toTry
}
