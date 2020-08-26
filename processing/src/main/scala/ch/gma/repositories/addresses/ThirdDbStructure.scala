package ch.gma.repositories.addresses

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 * This maps the structure of the database provided by the third application
 */
object ThirdDbStructure {

  type DataSetConverter = Dataset[Row] => Dataset[Row]

  // for each row, we are dropping the rec_act (actually the type of the record) before passing through a schema
  // mapper.
  val structures: Map[String, StructType] = Map(
    "01" -> StructType(
      StructField("rec", StringType, nullable = false) ::
        StructField("onrp", StringType, nullable = false) ::
        StructField("bfsnr", StringType, nullable = false) ::
        StructField("plz_typ", StringType, nullable = false) ::
        StructField("plz", StringType, nullable = false) ::
        StructField("plz_zz", StringType, nullable = false) ::
        StructField("gplz", StringType, nullable = false) ::
        StructField("ort_bez_18", StringType, nullable = false) ::
        StructField("ort_bez_27", StringType, nullable = false) ::
        StructField("kanton", StringType, nullable = false) ::
        StructField("sparchcode", StringType, nullable = false) ::
        StructField("sparchcode_abw", StringType, nullable = true) ::
        StructField("briefz_durch", StringType, nullable = true) ::
        StructField("gilt_ab_dat", StringType, nullable = false) ::
        StructField("plz_briefzust", StringType, nullable = false) ::
        StructField("plz_coff", StringType, nullable = true) ::
        Nil),
    "02" -> StructType(
      StructField("rec", StringType, nullable = false) ::
        StructField("onrp", StringType, nullable = false) ::
        StructField("laufnumber", StringType, nullable = false) ::
        StructField("bez_typ", StringType, nullable = false) ::
        StructField("sprachcode", StringType, nullable = false) ::
        StructField("ort_bez_18", StringType, nullable = false) ::
        StructField("ort_bez_27", StringType, nullable = false) ::
        Nil),
    "03" -> StructType(
      StructField("rec", StringType, nullable = false) ::
        StructField("bfsnr", StringType, nullable = false) ::
        StructField("gemeindename", StringType, nullable = false) ::
        StructField("kanton", StringType, nullable = false) ::
        StructField("agglonr", StringType, nullable = true) ::
        Nil),
    "04" -> StructType(
      StructField("rec", StringType, nullable = false) ::
        StructField("str_id", StringType, nullable = false) ::
        StructField("onrp", StringType, nullable = false) ::
        StructField("str_bez_k", StringType, nullable = false) ::
        StructField("str_bez_l", StringType, nullable = false) ::
        StructField("str_bez_2k", StringType, nullable = false) ::
        StructField("str_bez_2l", StringType, nullable = false) ::
        StructField("str_lok_typ", StringType, nullable = false) ::
        StructField("str_bez_spc", StringType, nullable = false) ::
        StructField("str_bez_coff", StringType, nullable = false) ::
        StructField("str_ganzfach", StringType, nullable = true) ::
        StructField("str_fach_onrp", StringType, nullable = true) ::
        Nil),
    "05" -> StructType(
      StructField("rec", StringType, nullable = false) ::
        StructField("str_id_alt", StringType, nullable = false) ::
        StructField("str_id", StringType, nullable = false) ::
        StructField("str_typ", StringType, nullable = false) ::
        StructField("str_bez_ak", StringType, nullable = false) ::
        StructField("str_bez_al", StringType, nullable = false) ::
        StructField("str_bez_a2k", StringType, nullable = false) ::
        StructField("str_bez_a2l", StringType, nullable = false) ::
        StructField("str_lok_typ", StringType, nullable = false) ::
        StructField("str_bez_spc", StringType, nullable = false) ::
        Nil),
    "06" -> StructType(
      StructField("rec", StringType, nullable = false) ::
        StructField("hauskey", StringType, nullable = false) ::
        StructField("str_id", StringType, nullable = false) ::
        StructField("hnr", StringType, nullable = true) ::
        StructField("hnr_a", StringType, nullable = true) ::
        StructField("hnr_coff", StringType, nullable = false) ::
        StructField("ganzfach", StringType, nullable = true) ::
        StructField("fach_onrp", StringType, nullable = true) ::
        Nil),
    "07" -> StructType(
      StructField("rec", StringType, nullable = false) ::
        StructField("hauskey_alt", StringType, nullable = false) ::
        StructField("hauskey", StringType, nullable = false) ::
        StructField("geb_bez_alt", StringType, nullable = false) ::
        StructField("geb_typ", StringType, nullable = false) ::
        Nil),
    "08" -> StructType(
      StructField("rec", StringType, nullable = false) ::
        StructField("hauskey", StringType, nullable = false) ::
        StructField("a_plz", StringType, nullable = false) ::
        StructField("bbz_plz", StringType, nullable = false) ::
        StructField("boten_bbz", StringType, nullable = false) ::
        StructField("etappen", StringType, nullable = false) ::
        StructField("lauf_nr", StringType, nullable = false) ::
        StructField("ndepot", StringType, nullable = true) ::
        Nil)
  )

  val headers = Map(
    "01" -> Array("rec_art", "onrp", "bfsnr", "plz_typ", "plz", "plz_zz", "gplz", "rt_bez_18", "ort_bez_27", "kanton",
      "sprachcode", "sprachcode_abw", "briefz_durch", "gilt_ab_dat", "plz_briefzust", "plz_coff"),
    "02" -> Array("rec_art", "onrp", "laufnummer", "bez_typ", "sprachcode", "rt_bez_18", "ort_bez_27"),
    "03" -> Array("rec_art", "bfsnr", "gemeindename", "kanton", "agglonr"),
    "04" -> Array("rec_art", "str_id", "onrp", "str_bez_k", "str_bez_l", "str_bez_2k", "str_bez_2l", "str_lok_typ",
      "str_bez_spc", "str_bez_coff", "str_ganzfach", "str_fach_onrp"),
    "05" -> Array("rec_act", "str_id_alt", "str_id", "str_typ", "str_bez_ak", "str_bez_al", "str_bez_a2k", "str_bez_a2l",
      "str_lok_typ", "str_bez_spc"),
    "06" -> Array("rec_art", "hauskey", "str_id", "hnr", "hnr_a", "hnr_coff", "ganzfach", "fach_onrp"),
    "07" -> Array("rec_art", "hauskey_alt", "hauskey", "geb_bez_alt", "geb_typ"),
    "08" -> Array("rec_art", "hauskey", "a_plz", "bbz_plz", "boten_bbz", "etapen_nr", "lauf_nr", "ndepot"),
  )

  def generateSplitter(s: String): Dataset[Row] => Dataset[Row] = (r: Dataset[Row]) => r.filter(r => r(0) == s)

  val splitters: Map[String, DataSetConverter] = Map(
    "01" -> generateSplitter("01"),
    "02" -> generateSplitter("02"),
    "03" -> generateSplitter("03"),
    "04" -> generateSplitter("04"),
    "05" -> generateSplitter("05"),
    "06" -> generateSplitter("06"),
    "07" -> generateSplitter("07"),
    "08" -> generateSplitter("08")
  )

  // NEW_PLZ1 <-
  def remap01(r: Row): Row = r

  // NEW_PLZ2
  def remap02(r: Row): Row = Row(r(0), r(1), r(2), r(3), r(4), r(6))

  // NEW_COM <-
  def remap03(r: Row): Row = Row(r(0), r(1), r(2), r(3), r(4))

  // NEW_STR <-
  def remap04(r: Row): Row = Row(r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8), r(9), r(10), r(11), r(12))

  // NEW_PLZ1
  def remap05(r: Row): Row = Row(r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8), r(9))

  // NEW_GEB <-
  def remap06(r: Row): Row = Row(r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8))

  // NEW_GEBA
  def remap07(r: Row): Row = Row(r(0), r(1), r(2), r(3), r(4))

  // NEW_BOT_B
  def remap08(r: Row): Row = Row(r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7))

  val remappers: Map[String, (Row) => Row] = Map(
    "01" -> remap01,
    "02" -> remap02,
    "03" -> remap03,
    "04" -> remap04,
    "05" -> remap05,
    "06" -> remap06,
    "07" -> remap07,
    "08" -> remap08
  )

  def perform(r: Dataset[Row])(implicit spark: SparkSession): Unit = {
    // at first create view for all
    Array("01", "02", "03", "04", "05", "06", "07", "08").foreach(recArt => {
      spark.createDataFrame(splitters(recArt)(r)
        .rdd.map(remappers(recArt)), structures(recArt))
        .createOrReplaceTempView(s"v${recArt}")
    })

    // TODO: improve performance since the cartesian product is a bit huge to fit in memory of a strandalone cluster
    // TODO: select only relevant columns for final indexing
    spark.sql("select * from v01 " +
      "join v04 on v01.onrp == v04.onrp " +
      "join v06 on v04.str_id == v06.str_id").show
  }

}
