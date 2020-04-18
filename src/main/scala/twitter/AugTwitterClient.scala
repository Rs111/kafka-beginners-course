package twitter

import com.twitter.hbc.core.Client

import scala.util.Try

trait AugTwitterClient {
  val client: Client

  def connect(): Try[Unit] = Try(client.connect())
  def isDone: Boolean = client.isDone
  def stop(): Unit = client.stop()
}

object AugTwitterClient {
  def apply(_client: Client): AugTwitterClient = new AugTwitterClient {
    override val client: Client = _client
  }
}
