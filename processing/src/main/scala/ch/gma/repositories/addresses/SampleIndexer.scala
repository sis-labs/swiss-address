package ch.gma.repositories.addresses

import java.util.Properties

import org.apache.spark.sql.{Row, SparkSession}
import spray.json.DefaultJsonProtocol

/**
 * This is a sample to show how the indexing behaves
 */
object SampleIndexer {

  case class Singer(name: String, date: String, gender: String, album: Int)

  object MyJsonProtocol extends DefaultJsonProtocol {
    implicit val colorFormat = jsonFormat4(Singer)
  }

  import MyJsonProtocol._
  import spray.json._

  val data = Seq(("Bruce Springsteen","1963","M",30),
    ("Janis Joplin","1962","F",7),
    ("Jimi Hendrix","1953","M",10),
    ("Marvin Gaye","1965","M",10),
    ("James Brown","1956","M",20)
  )

  def fromRaw(tuple: (String, String, String, Int)): Singer = Singer(tuple._1, tuple._2, tuple._3, tuple._4)
  def fromRow(row: Row) : Singer = Singer(row.getAs[String](0), row.getAs[String](1), row.getAs[String](2), row.getAs[Int](3))
  def toJson(s:Singer): String = s.toJson.toString()

  def getKafkaConfig(hosts: String, port: Int): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("metadata.broker.list", "localhost:9092")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("producer.type", "async")
    props.put("request.required.acks", "0")
    props.put("queue.buffering.max.ms", "5000")
    props.put("queue.buffering.max.messages", "2000")

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "1")

    props.put("retries", "3")
    //props.put("linger.ms", "5")
    props
  }

  def run()(implicit spark: SparkSession) : Unit = {
    import spark.sqlContext.implicits._

    val kafkaSink = spark.sparkContext.broadcast(KafkaSink(getKafkaConfig("localhost", 9092)))
    val kafkaTopic = "addressXes"

    val df = data.toDF("name","dob_year","gender","albums")
    df.printSchema()
    df.show(false)

    val p = data.map(fromRaw).toDF("Name", "year", "Gender", "Albums")
    p.rdd.foreach( row => {
      val s = toJson(fromRow(row))
      println(s);
      kafkaSink.value.send(kafkaTopic, s);
    })
  }
}
