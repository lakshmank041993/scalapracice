import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object week12SqlOrStringExpressionUDT extends App {

  // create a named function for checking the age

  def ageCheck(age:Int) :String = {
    if (age > 18) "Y" else "N"
  }

  //spark config
  val myConf = new SparkConf()
  myConf.set("spark.app.name","udf in spark sql")
  myConf.set("spark.master","local[*]")

  // spark session
  val spark = SparkSession.builder.config(myConf).getOrCreate()

  //csv load
  val csvLoad = spark.read.format("csv").option("inferSchema",true).option("path","/home/rithwick/Downloads/name.csv").load
  // name the custom headers
  val csvLoadDf =csvLoad.toDF("name","age","city")
  // create spark sql udf
  // register the udf
  spark.udf.register("parseAgeCheck",ageCheck(_:Int):String)

  // check the function is registered in catalog

  spark.catalog.listFunctions.filter(x => x.name == "parseAgeCheck").show
  //create a database in spark sql

  csvLoadDf.createOrReplaceTempView("agecheck")

  spark.sql("select name ,age ,city, parseAgeCheck(age) as is_adult from agecheck").show

  spark.stop()


}



