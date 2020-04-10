import java.util.Properties

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer

/* Three steps to create producer
  1 - create producer properties
  2 - create producer
  3 - send data

  Callbacks
    - wouldn't it be nice to understand where message was produced, if it was produced correctly, etc?
*/

class Producer(topic: String) {
//  def main(args: Array[String]): Unit = {
//    writeToKafka("quick-start")
//  }

  def writeToKafka(message: (String, String)): Unit = {
    /* 1
      Create Producer Properties
      - to create producer properties: java.util.Properties
      - what properties do we put in?
      - go to kafka docs: https://kafka.apache.org/documentation/
      - 3.3 Producer Configs
      - serializers help producer know what kind of value you are sending to kafka and how this should be serialized to bytes
      - kafka client will convert whatever we send to kafka to bytes (0s and 1s)
      - for our case, we are going to send strings
        - we need string serializer for the key and string serializer for the value
     */
    val props = new Properties()
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    // OLD WAY
//    props.put("bootstrap.servers", "127.0.0.1:9092") // or localhost:9094
//    props.put("key.serializer", classOf[StringSerializer].getName) // "org.apache.kafka.common.serialization.StringSerializer"
//    props.put("value.serializer", classOf[StringSerializer].getName) //"org.apache.kafka.common.serialization.StringSerializer"


    /* 2
      Create producer
      - String,String because we are producing (k:String, v:String)
     */
    val producer: KafkaProducer[String,String] = new KafkaProducer[String, String](props)

    /* 3
      Send Data
      - create a producer record
      - send it to kafka
      - close producer
      - start consol consumer and see if anything happens
        kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic first_topic --group my-first-application
     */
    val record = new ProducerRecord[String, String](topic, message._1, message._2)
    // send is asynchronous (happens in background); as this is executing, the program exit and data never sends
    // need to add close or flush after this to make sure record is done sending before stopping app
    producer.send(record)

    // flush data: wait for data to be produced
    producer.flush()
    // flush and close producer
    producer.close()
  }
}