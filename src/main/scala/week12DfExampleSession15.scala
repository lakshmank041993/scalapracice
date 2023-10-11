import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, unix_timestamp}
import org.apache.spark.sql.types.DateType

import java.util.logging.{Level, Logger}

object week12DfExampleSession15 extends App {
  // set Logger level
  Logger.getLogger("org").setLevel(Level.SEVERE)
  // create spar conf
  val sConf = new SparkConf()
  sConf.set("spark.app.name","df Example")
  sConf.set("spark.master","local[2]")

  // create sparkSession
  val spark = SparkSession.builder().config(sConf).getOrCreate()

  // convert scala list to data frame
  //we have scala list below
  val sampleList =List(
    (1,"2013-07-25",11599,"CLOSED"),
    (2,"2013-07-25",115,"PAYMENT_PENDING"),
    (3,"2013-07-25",11599,"COMPLETED"),
    (4,"2013-07-25",1199,"CLOSED"))
  import spark.implicits._
  /* we will create a dataFrame using RDD
  import spark.implicits._
  val sparkRdd = spark.sparkContext.parallelize(SampleList)
  sparkRdd.toDF()
   */
  // using createDataFrame function
  // createDataFrame implicitly infers schema , to add the column names = toDf(columns)

  val orderDf = spark.createDataFrame(sampleList).toDF("orderId","orderDate","customerId","status")
  orderDf.show()
  orderDf.printSchema()
  // now we need to add or alter existing column , if i want to process one column and process date
  // use .withColumn to change the value

  val newDf = orderDf.withColumn("orderDate",unix_timestamp(col("orderDate").cast(DateType)))

  newDf.show()
  newDf.printSchema()
  val newDf2 = newDf.withColumn("newid",monotonically_increasing_id())
  newDf2.show()
  newDf.printSchema()

  // to drop duplicates using - drop duplicates from column orderDate and orderDate

  val dropDupDf = newDf2.dropDuplicates("orderDate","customerId")
  // create a new column and it show
  dropDupDf.show()

  // drop the colum of orderid

  val dropOrderId = dropDupDf.drop("orderId")

  dropOrderId.show()

  // sort the data with orderData

  val sortByOrderDate = dropOrderId.sort("orderDate")
  sortByOrderDate.show()

  // stop scala session
  spark.stop()
}
