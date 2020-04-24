package elasticsearch

import java.time.Duration

import grizzled.slf4j.{Logger, Logging}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.elasticsearch.action.bulk.{BulkRequest, BulkResponse}
import org.elasticsearch.action.index.{IndexRequest, IndexResponse}
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

object Main extends App with Logging {



  val client: RestHighLevelClient = createClient(hostName, userName, password)




  val kafkaConsumer = createKafkaConsumer("twitter_tweets")




  while (true) {
    Thread.sleep(5000)
    val records: Iterable[ConsumerRecord[String, String]] = kafkaConsumer.poll(Duration.ofMillis(100)).asScala
    val recordCount = records.toList.length
    logger.info("Received " + recordCount + " records")

    // bulk request
    val bulkRequest: BulkRequest = new BulkRequest()

    for (record <- records.iterator if record != null && record.value() != null) {
      println("print records")
      println(record.value())
      Thread.sleep(5000)
      println("finished sleeping")
      logger.info( "\n" +
        "Key: " + record.key() + "\n" +
        "Value: " + record.value() + "\n" +
        "Partition: " + record.partition() + "\n" +
        "Offset: " + record.offset()
      )

      // Insert data into elastic search
      // index, type, and id; must exist or this will fail
      // add and control ID arguement to ensure idempotence
      // i.e. right now you might send same message twice with a different random id, but if you derive id from message content, if you send twice the first message will be over-written
      // e.g. id: String = record.topic() + "_" + record.partition() + "_" + record.offset()
      // twitter feed specific id; id = extractIdFromTweetId(record.value()); e.g. with circe
      val indexRequest: IndexRequest =
        new IndexRequest("twitter")
          .`type`("tweets")
          .id(record.topic() + "_" + record.partition() + "_" + record.offset())
          .source(record.value(), XContentType.JSON)
//        new IndexRequest("twitter", "tweets")
//          indexRequest.source(record.value(), XContentType.JSON)

      bulkRequest.add(indexRequest) // add to bulk request

//      // send data and get a response
//      val indexResponse: IndexResponse = client.index(indexRequest, RequestOptions.DEFAULT)
//      val id: String = indexResponse.getId
//      logger.info(id)
//      kafkaConsumer.commitSync()
      Thread.sleep(1000)
    }
    if (recordCount > 0) {
      val bulkItemResponse: BulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT)
      kafkaConsumer.commitSync()
    }

  }




  client.close()
}
