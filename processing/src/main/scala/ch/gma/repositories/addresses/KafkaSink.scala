package ch.gma.repositories.addresses

import java.util.Properties
import org.apache.kafka.clients.producer._
import scala.collection.JavaConverters._

class KafkaSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {

  lazy val producer: KafkaProducer[String, String] = createProducer()
  def send(topic: String, value: String) : Unit = producer.send(new ProducerRecord(topic, value))
}

object KafkaSink {
  def apply(config: Properties): KafkaSink = {
    val f = () => {
      val p = new KafkaProducer[String, String](config)
      sys.addShutdownHook {
        p.close()
      }
      p
    }
    new KafkaSink(f)
  }
}
