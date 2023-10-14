import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


case class Logging(level:String , datetime:String)


object week12bigLohExample extends App {

  // conf
  val conf = new SparkConf()
  conf.set("spark.app.name","big log eg")
  conf.set("spark.master","local[2]")

  // spark session

  val spark = SparkSession.builder().config(conf).getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  // load a sample list to test out my code
  /*
  val myList = List("WARN,2016-12-31 04:19:32",
    "FATAL,2016-12-31 03:22:32",
    "WARN,2016-12-31 03:21:32",
    "INFO,2015-4-21 14:32:32",
    "FATAL,2016-1-31 03:22:32",
    "ERROR,2016-5-31 04:19:32")

  import spark.implicits._

  // loading a list to rdd
  val rdd = spark.sparkContext.parallelize(myList)

  // convert this to a rdd
  // call a map class , mapper
  def mapper(line:String):Logging ={
    val fields =line.split(",")
    val logging:Logging = Logging(fields(0),fields(1))
    logging
  }

  val rdd2 = rdd.map(mapper)

  val df1 = rdd2.toDF()
  //df1.show()

  df1.createOrReplaceTempView("logging_table")
  //spark.sql("select * from logging_table").show()

  //spark.sql("select level ,count(datetime) from logging_table group by level order by level").show(false)

  //extract the month
  val df2 = spark.sql("select level, date_format(datetime,'MMM') as month from logging_table ")

  df2.createOrReplaceTempView("new_logging_table")

  spark.sql("select level , month, count(1) from new_logging_table group by level , month").show()
*/

  val df3 = spark.read.format("csv").option("header","true").option("path","/home/rithwick/Downloads/import.cdn.thinkific.com_349536_biglog-201105-152517.txt").load()

  // df3.show()
  df3.createOrReplaceTempView("my_logging_table")
  val df4 = spark.sql(
    """select
      |level, date_format(datetime,'MMM') as month, cast(first(date_format(datetime,'M')) as int) as month_num, count(1) as total
      |from my_logging_table group by level, month order by month_num """.stripMargin)

  val df5 =df4.drop("month_num")

  df5.createOrReplaceTempView("results_table")
  spark.sql("select * from results_table").show(60)

  // think of a pivot table
 spark.sql(
    """select
      |level, date_format(datetime,'MMM') as month, cast((date_format(datetime,'M')) as int) as month_num
      |from my_logging_table""".stripMargin).groupBy("level").pivot("month_num").count().show(100)

  // system has to run internally distint count in case to show case month

  val columns = List("January","February","March","April","May","June","July","August","September","October","November","December")


  spark.sql(
    """select
      |level, date_format(datetime,'MMMM') as month
      |from my_logging_table""".stripMargin).groupBy("level").pivot("month",columns).count().show(100)


  spark.stop()

}


/*
+-----+-----+-----+
|level|month|total|
+-----+-----+-----+
| INFO|  Jan|29119|
|ERROR|  Jan| 4054|
| WARN|  Jan| 8217|
|FATAL|  Jan|   94|
|DEBUG|  Jan|41961|
|DEBUG|  Feb|41734|
| INFO|  Feb|28983|
|FATAL|  Feb|   72|
| WARN|  Feb| 8266|
|ERROR|  Feb| 4013|
|DEBUG|  Mar|41652|
|ERROR|  Mar| 4122|
|FATAL|  Mar|   70|
| INFO|  Mar|29095|
| WARN|  Mar| 8165|
|FATAL|  Apr|   83|
| INFO|  Apr|29302|
| WARN|  Apr| 8277|
|DEBUG|  Apr|41869|
|ERROR|  Apr| 4107|
|FATAL|  May|   60|
|DEBUG|  May|41785|
|ERROR|  May| 4086|
| INFO|  May|28900|
| WARN|  May| 8403|
|ERROR|  Jun| 4059|
|FATAL|  Jun|   78|
| WARN|  Jun| 8191|
|DEBUG|  Jun|41774|
| INFO|  Jun|29143|
| INFO|  Jul|29300|
| WARN|  Jul| 8222|
|FATAL|  Jul|   98|
|DEBUG|  Jul|42085|
|ERROR|  Jul| 3976|
|FATAL|  Aug|   80|
| INFO|  Aug|28993|
|DEBUG|  Aug|42147|
|ERROR|  Aug| 3987|
| WARN|  Aug| 8381|
|DEBUG|  Sep|41433|
|ERROR|  Sep| 4161|
| WARN|  Sep| 8352|
|FATAL|  Sep|   81|
| INFO|  Sep|29038|
|ERROR|  Oct| 4040|
| INFO|  Oct|29018|
| WARN|  Oct| 8226|
|FATAL|  Oct|   92|
|DEBUG|  Oct|41936|
|FATAL|  Nov|16797|
| INFO|  Nov|23301|
| WARN|  Nov| 6616|
|DEBUG|  Nov|33366|
|ERROR|  Nov| 3389|
|DEBUG|  Dec|41749|
|ERROR|  Dec| 4106|
|FATAL|  Dec|   94|
| INFO|  Dec|28874|
| WARN|  Dec| 8328|
+-----+-----+-----+

+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
|level|    1|    2|    3|    4|    5|    6|    7|    8|    9|   10|   11|   12|
+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
| INFO|29119|28983|29095|29302|28900|29143|29300|28993|29038|29018|23301|28874|
|ERROR| 4054| 4013| 4122| 4107| 4086| 4059| 3976| 3987| 4161| 4040| 3389| 4106|
| WARN| 8217| 8266| 8165| 8277| 8403| 8191| 8222| 8381| 8352| 8226| 6616| 8328|
|FATAL|   94|   72|   70|   83|   60|   78|   98|   80|   81|   92|16797|   94|
|DEBUG|41961|41734|41652|41869|41785|41774|42085|42147|41433|41936|33366|41749|
+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+

+-----+-------+--------+-----+-----+-----+-----+-----+------+---------+-------+--------+--------+
|level|January|February|March|April|  May| June| July|August|September|October|November|December|
+-----+-------+--------+-----+-----+-----+-----+-----+------+---------+-------+--------+--------+
| INFO|  29119|   28983|29095|29302|28900|29143|29300| 28993|    29038|  29018|   23301|   28874|
|ERROR|   4054|    4013| 4122| 4107| 4086| 4059| 3976|  3987|     4161|   4040|    3389|    4106|
| WARN|   8217|    8266| 8165| 8277| 8403| 8191| 8222|  8381|     8352|   8226|    6616|    8328|
|FATAL|     94|      72|   70|   83|   60|   78|   98|    80|       81|     92|   16797|      94|
|DEBUG|  41961|   41734|41652|41869|41785|41774|42085| 42147|    41433|  41936|   33366|   41749|
+-----+-------+--------+-----+-----+-----+-----+-----+------+---------+-------+--------+--------+


Process finished with exit code 0

 */