import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CorroptedDataLoadDropMalFormed extends App {

  //default mode is DROPMALFORMED

  // set spark config
  val sConf = new SparkConf()
  sConf.set("spark.app.name", "dataFrame Example")
  sConf.set("spark.master", "local[*]")
  // create spark session
  val sSession = SparkSession.builder().config(sConf).getOrCreate()

  // create dataframe
  // below is a data frame reader
  val orderDF = sSession.read.format("json").option("mode","DROPMALFORMED")
    .option("path", "/home/rithwick/Downloads/players-201019-002101.json").load
  // import with sparkSession

  orderDF.show
  orderDF.printSchema
  orderDF.count()
  //orderDF.filter("order_id < 10").show // give op since column name is matching
  //orderDF.filter("order_ids < 10").show //gives run time error since headers is not matching
  scala.io.StdIn.readLine()
  sSession.stop()

  /*
  +---+-------+---------+-----------+----------+-------+
  |age|country|player_id|player_name|role      |team_id|
  +---+-------+---------+-----------+----------+-------+
  |33 |IND    |101      |R Sharma   |Batsman   |11     |
  |25 |IND    |102      |S Iyer     |Batsman   |15     |
  |30 |NZ     |103      |T Boult    |Bowler    |13     |
  |38 |IND    |104      |MS Dhoni   |WKeeper   |14     |
  |39 |AUS    |105      |S Watson   |Allrounder|12     |
  +---+-------+---------+-----------+----------+-------+

   */

}
