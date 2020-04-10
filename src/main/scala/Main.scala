object Main extends App {

  val producer = new ProducerWithCallbackAndKeys("first_topic")
  val consumer = new ConsumerWithThreads("first_topic")

  //dumb
  //(0 until 10).foreach(i => producer.writeToKafka(("my", "cheese" + i)))

  //producer.writeToKafka()
  consumer.consumeNew()
}
