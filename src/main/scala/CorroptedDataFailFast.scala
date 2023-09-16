import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CorroptedDataFailFast extends App {

  //default mode is FAILFAST

  // set spark config
  val sConf = new SparkConf()
  sConf.set("spark.app.name", "dataFrame Example")
  sConf.set("spark.master", "local[*]")
  // create spark session
  val sSession = SparkSession.builder().config(sConf).getOrCreate()

  // create dataframe
  // below is a data frame reader
  val orderDF = sSession.read.format("json").option("mode", "FAILFAST")
    .option("path", "/home/rithwick/Downloads/players-201019-002101.json").load
  // import with sparkSession

  orderDF.show(false)
  orderDF.printSchema
  orderDF.count()
  //orderDF.filter("order_id < 10").show // give op since column name is matching
  //orderDF.filter("order_ids < 10").show //gives run time error since headers is not matching
  scala.io.StdIn.readLine()
  sSession.stop()
  //exception is raised if data is malformed
  /*
  23/09/02 19:48:49 INFO SparkUI: Stopped Spark web UI at http://192.168.0.119:4041
  Exception in thread "main" org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 0.0 failed 1 times, most recent failure: Lost task 0.0 in stage 0.0 (TID 0) (192.168.0.119 executor driver): org.apache.spark.SparkException: Malformed records are detected in schema inference. Parse Mode: FAILFAST.
    at org.apache.spark.sql.errors.QueryExecutionErrors$.malformedRecordsDetectedInSchemaInferenceError(QueryExecutionErrors.scala:1325)
    at org.apache.spark.sql.catalyst.json.JsonInferSchema.handleJsonErrorsByParseMode(JsonInferSchema.scala:64)
    at org.apache.spark.sql.catalyst.json.JsonInferSchema.$anonfun$infer$2(JsonInferSchema.scala:93)
    at scala.collection.Iterator$$anon$11.nextCur(Iterator.scala:486)
    at scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:492)
    at scala.collection.Iterator.foreach(Iterator.scala:943)
    at scala.collection.Iterator.foreach$(Iterator.scala:943)
    at scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
    at scala.collection.TraversableOnce.reduceLeft(TraversableOnce.scala:237)
    at scala.collection.TraversableOnce.reduceLeft$(TraversableOnce.scala:220)

   */


}
