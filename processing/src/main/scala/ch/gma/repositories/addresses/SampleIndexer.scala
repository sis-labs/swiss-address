package ch.gma.repositories.addresses

import org.apache.spark.sql.SparkSession

/**
 * This is a sample to show how the indexing behaves
 */
object SampleIndexer {
  case class Singer(name: String, date: String, gender: String, album: Int)

  val data = Seq(("Bruce Springsteen","1963","M",30),
    ("Janis Joplin","1962","F",7),
    ("Jimi Hendrix","1953","M",10),
    ("Marvin Gaye","1965","M",10),
    ("James Brown","1956","M",20)
  )

  def fromRaw(tuple: (String, String, String, Int)): Singer = Singer(tuple._1, tuple._2, tuple._3, tuple._4)

  def run()(implicit spark: SparkSession) : Unit = {
    import spark.sqlContext.implicits._
    val df = data.toDF("name","dob_year","gender","albums")
    df.printSchema()
    df.show(false)

    val p = data.map(fromRaw).toDF("Name", "year", "Gender", "Albums")
    p.foreachPartition( partition => {
      val kafkaTopic = "part.referentials.addressses"
    })
  }
}
