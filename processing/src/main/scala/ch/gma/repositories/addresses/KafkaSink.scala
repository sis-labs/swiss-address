package ch.gma.repositories.addresses

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class KafkaSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {

  lazy val producer = createProducer()
  def send(topic: String, value: String) : Unit = producer.send(new ProducerRecord(topic, value))
}

object KafkaSink {
  def buildKafkaConfig(hosts: String, port: Int): Properties = {
    val props = new Properties()
    props.put("metadata.broker.list", "localhost:9092")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("producer.type", "async")
    props.put("request.required.acks", "0")
    props.put("queue.buffering.max.ms", "5000")
    props.put("queue.buffering.max.messages", "2000")
    props
  }
  def apply(config: Map[String, Object]): KafkaSink = {
    val f = () => {
      val p: KafkaProducer[String, String] = new KafkaProducer[String, String](config)
      sys.addShutdownHook {
        p.close()
      }
      p
    }
    new KafkaSink(f)
  }
}
