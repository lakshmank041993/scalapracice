import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CorruptedRecordLoadDefaultMode extends App {


  //default mode is PERMISSIVE

  // set spark config
  val sConf = new SparkConf()
  sConf.set("spark.app.name", "dataFrame Example")
  sConf.set("spark.master", "local[*]")
  // create spark session
  val sSession = SparkSession.builder().config(sConf).getOrCreate()

  // create dataframe
  // below is a data frame reader
  val orderDF = sSession.read.format("json")
    .option("path", "/home/rithwick/Downloads/players-201019-002101.json").load
  // import with sparkSession

  orderDF.show(false)
  orderDF.printSchema
  orderDF.count()
  //orderDF.filter("order_id < 10").show // give op since column name is matching
  //orderDF.filter("order_ids < 10").show //gives run time error since headers is not matching
  scala.io.StdIn.readLine()
  sSession.stop()

  // out put
  /*
  +--------------------+----+-------+---------+-----------+----------+-------+
  |     _corrupt_record| age|country|player_id|player_name|      role|team_id|
  +--------------------+----+-------+---------+-----------+----------+-------+
  |                null|  33|    IND|      101|   R Sharma|   Batsman|     11|
  |                null|  25|    IND|      102|     S Iyer|   Batsman|     15|
  |                null|  30|     NZ|      103|    T Boult|    Bowler|     13|
  |                null|  38|    IND|      104|   MS Dhoni|   WKeeper|     14|
  |                null|  39|    AUS|      105|   S Watson|Allrounder|     12|
  |{"player_id":106,...|null|   null|     null|       null|      null|   null|
  +--------------------+----+-------+---------+-----------+----------+-------+
   */




}
