
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}



object week11Assgnment extends App {

  val myconf =new SparkConf()
  myconf.set("spark.app.name","assignment11")
  myconf.set("spark.master","local[*]")

  val header = StructType(List(
    StructField("Country",StringType),
    StructField("weeknum",IntegerType),
    StructField("numinvoice",IntegerType),
    StructField("totalquantity",IntegerType),
    StructField("invoice",DoubleType)
     ))

  val spark = SparkSession.builder().config(myconf).getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  val windowfileLoad =spark.read.format("csv").schema(header).option("path","file:///home/rithwick/Downloads/windowdata-201021-002706.csv").load()
  println("csv load successfull")
  // print value into in parquet format
  //default save is done by parque
  windowfileLoad.write.partitionBy("Country","weeknum").mode(SaveMode.Overwrite).option("path","file:///home/rithwick/Downloads/parquet_week11_assignment_op").save()
  println("parquet file saved successfully")
  //default save is done by avro
    windowfileLoad.write.format("avro").partitionBy("Country").mode(SaveMode.Overwrite).option("path","/home/rithwick/Downloads/avro_week11_assignment_op").save()
  println("avro file saved successfully")
  spark.stop()
}
