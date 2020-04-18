package twitter

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}

import com.twitter.hbc.httpclient.auth.{Authentication, OAuth1}
import grizzled.slf4j.Logging
import twitter.parser.ArgParser

import scala.util.{Failure, Success, Try}

object Main extends App with Logging {

  val msgQueue: BlockingQueue[String] = new LinkedBlockingQueue[String](1000)

//  for {
//    cliArgs <- ArgParser.parse(args)
//    twitterClient <- createHosebirdClient(new OAuth1(cliArgs.consumerKey, cliArgs.consumerSecret, cliArgs.token, cliArgs.secret), msgQueue)
//    augTwitterClient: AugTwitterClient = AugTwitterClient(twitterClient)
//    _ <- augTwitterClient.connect()
//    augKafkaProducer = AugKafkaProducer(cliArgs.outputTopic, createKafkaProducer())
//
//  } yield {
//
//
//  }

  /** cliArgs **/
  val cliArgs = ArgParser.parse(args).get

  /** create twitter client **/
  val hosebirdAuth: Authentication = new OAuth1(cliArgs.consumerKey, cliArgs.consumerSecret, cliArgs.token, cliArgs.secret)
  //val msgQueue: BlockingQueue[String] = new LinkedBlockingQueue[String](1000)
  val twitterClient = AugTwitterClient(createHosebirdClient(hosebirdAuth, msgQueue).get)
  twitterClient.connect()

  /** create kafka producer **/
  val augKafkaProducer = AugKafkaProducer(cliArgs.outputTopic, createKafkaProducer())

  /** shutdown hook **/
  Runtime
    .getRuntime
    .addShutdownHook(
      new Thread(() => {
        logger.info("stopping application...")
        logger.info("shutting down client from twitter...")
        twitterClient.client.stop()
        logger.info("closing producer...")
        augKafkaProducer.producer.close()
        logger.info("shutdown complete")
      })
    )

  /** loop to poll from msgQueue where twitter client is sending tweets and send to kafka **/
  while (!twitterClient.isDone) {
    Try(msgQueue.poll(5, TimeUnit.SECONDS)) match {
      case Failure(e: InterruptedException) =>
        logger.error("interruption exception")
        twitterClient.stop()
        e.printStackTrace()
      case Success(value) =>
        println(value)
        augKafkaProducer.send(value)
    }
  }
}
