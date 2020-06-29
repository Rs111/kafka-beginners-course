import java.util
import java.util.Properties

import org.apache.http.HttpHost
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.CredentialsProvider
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.elasticsearch.client.{RestClient, RestClientBuilder, RestHighLevelClient}

package object elasticsearch {

  def createClient(hostName: String, userName: String, password: String): RestHighLevelClient = {

    val credentialsProvider: CredentialsProvider = new BasicCredentialsProvider()
    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password))

    val builder: RestClientBuilder =
      RestClient
        .builder(new HttpHost(hostName, 443, "https"))
        .setHttpClientConfigCallback((httpClientBuilder: HttpAsyncClientBuilder) => {
          httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
        })

    new RestHighLevelClient(builder)
  }

  def createKafkaConsumer(topic: String): KafkaConsumer[String,String] = {
    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kafka-demo-elasticsearch-six") // if start one consumer instance in group, then start another in sae group, a rebalance would occur
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") // earliest, latest, none
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10")
    //props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)

    val consumer: KafkaConsumer[String,String] = new KafkaConsumer[String,String](props)
    consumer.subscribe(util.Arrays.asList(topic))

    consumer
  }
}
