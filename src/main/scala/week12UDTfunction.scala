import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{Dataset, Row, SparkSession}


// if i want ot convert ot data set

case class city(name:String,age:Int,city:String)

object week12UDTfunction extends App {

  def ageCheck(age: Int): String = {
    if (age > 20) "Y" else "N"
  }


  //spark conf
  val myConf  = new SparkConf()
  myConf.set("spark.app.name","UDT function")
  myConf.set("spark.master","local[*]")

  //spark Session

  val spark = SparkSession.builder().config(myConf).getOrCreate()
  import spark.implicits._
  val csvLoad = spark.read.format("csv").option("inferSchema",true).option("path","/home/rithwick/Downloads/name.csv").load

  val csvDf:Dataset[Row] = csvLoad.toDF("name","age","city")

  //csvDf.as[city].toDF().as[city].toDF()

  // need to create a user defined function in scala and add a column

  // def udf function
  val verifyAge = udf(ageCheck(_:Int):String)

  val addingNewCol = csvDf.withColumn("adult",verifyAge(col("age")))
  addingNewCol.show()

  spark.catalog.listFunctions.filter(x => x.name =="verifyAge").show



}
