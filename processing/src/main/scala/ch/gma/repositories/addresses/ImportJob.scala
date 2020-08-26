package ch.gma.repositories.addresses

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object ImportJob {

  val appName = "AddressImport"

  val inputFolder = s"${System.getProperty("user.home")}/Downloads/tmp"
  val outputFolder = "/tmp/swiss-address"

  // generate a uuid based id in a string format
  private[this] def id() = java.util.UUID.randomUUID().toString

  def main(args: Array[String]): Unit = {
    /*
    val conf = new SparkConf()
      .setAppName("AddressImport")
      .setMaster("local")

    val sc = new SparkContext(conf)
    */

    implicit val spark: SparkSession = SparkSession.builder()
      .appName(appName)
      .master("local")
      .config("spark.sql.ansi.enabled", "false")
      .getOrCreate()

    import spark.implicits._

    SampleIndexer.run()

    // TODO: validate input so that we can use the first arg as input file and following as output (fs, elastisearch, ...)
    /*
    val f = spark.read
      // due to the csv data, we have to force the encoding
      .option("charset", "ISO-8859-1")
      .option("encoding", "ISO-8859-1")

      // this is a ';' separated file
      .option("sep", ";")

      // TODO: use the first args of the cli
      .csv(s"${inputFolder}/raw_src.csv") // Post_Adressdaten20200721.work

    // the first col actually defines the table where the line comes from, so, repartition the set so that our Rdd maps table
    f.repartition(col("_c0"))
      // just for the test, output those information in the temp repository
      .write.csv(s"${outputFolder}/${id()}")
    */

    // perform the indexation process
    //ThirdDbStructure.perform(f)

  }
}
