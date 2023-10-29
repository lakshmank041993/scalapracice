import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

object week13_salting extends App {

  val random = new scala.util.Random
  val start = 1
  val end = 40
  val sc =new SparkContext("local[*]", "reduce key")

  val rdd1 = sc.textFile("/home/rithwick/Downloads/bigLog.txt")
  val rdd2 = rdd1.map(x => {
    val num = start + random.nextInt((end + start)+1)
    (x.split(":")(0)+num,x.split(":")(1))
  })
  val rdd3 = rdd2.groupByKey()

  val rdd4 = rdd3.map(x => (x._1,x._2.size))
  val rdd5 = rdd4.map(x => {
    if (x._1.substring(0,4) == "WARN") ("WARN",x._2) else ("ERROR",x._2)
  })
  val rdd6 = rdd5.reduceByKey((x,y) => x+y)
  rdd6.collect.foreach(println)

}

/*
scala> val random = new scala.util.Random
random: scala.util.Random = scala.util.Random@7f9ebcfe

scala>   val start = 1
start: Int = 1

scala>   val end = 40
end: Int = 40

scala>   val rdd1 = sc.textFile("biglogtxtnewlatest1.txt")
rdd1: org.apache.spark.rdd.RDD[String] = biglogtxtnewlatest1.txt MapPartitionsRDD[1] at textFile at <console>:24

scala>   val rdd2 = rdd1.map(x => {
     |     val num = start + random.nextInt((end + start)+1)
     |     (x.split(":")(0)+num,x.split(":")(1))
     |   })
rdd2: org.apache.spark.rdd.RDD[(String, String)] = MapPartitionsRDD[2] at map at <console>:31

scala>   val rdd3 = rdd2.groupByKey()
rdd3: org.apache.spark.rdd.RDD[(String, Iterable[String])] = ShuffledRDD[3] at groupByKey at <console>:25

scala>   val rdd4 = rdd3.map(x => (x._1,x._2.size))
rdd4: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[4] at map at <console>:25

scala>   val rdd5 = rdd4.map(x => {
     |     if (x._1.substring(0,4) == "WARN") ("WARN",x._2) else ("ERROR",x._2)
     |   })
rdd5: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[5] at map at <console>:25

scala>   val rdd6 = rdd5.reduceByKey((x,y) => x+y)
rdd6: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[6] at reduceByKey at <console>:25

scala>   rdd6.collect.foreach(println)
(WARN,119973264)
(ERROR,120026736)
 */