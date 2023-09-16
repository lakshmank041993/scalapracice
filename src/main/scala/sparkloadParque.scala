import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object sparkloadParque extends App {


  val conf = new SparkConf()
  conf.set("spark.app.name","load parque")
  conf.set("spark.master","local[*]")
  // create spark session
  val spark =SparkSession.builder().config(conf).getOrCreate()

  val parqueLoad = spark.read.option("path","/home/rithwick/Downloads/users-201019-002101.parquet").load
  parqueLoad.show(false)
  parqueLoad.printSchema
  spark.stop()


  /*
  -------------------+---+----------+---------+------------------------+------+---------------+-------------------+----------------------+----------+---------+----------------------------+----------------------------+
  |registration_dttm  |id |first_name|last_name|email                   |gender|ip_address     |cc                 |country               |birthdate |salary   |title                       |comments                    |
  +-------------------+---+----------+---------+------------------------+------+---------------+-------------------+----------------------+----------+---------+----------------------------+----------------------------+
  |2016-02-03 13:25:29|1  |Amanda    |Jordan   |ajordan0@com.com        |Female|1.197.201.2    |6759521864920116   |Indonesia             |3/8/1971  |49756.53 |Internal Auditor            |1E+02                       |
  |2016-02-03 22:34:03|2  |Albert    |Freeman  |afreeman1@is.gd         |Male  |218.111.175.34 |                   |Canada                |1/16/1968 |150280.17|Accountant IV               |                            |
  |2016-02-03 06:39:31|3  |Evelyn    |Morgan   |emorgan2@altervista.org |Female|7.161.136.94   |6767119071901597   |Russia                |2/1/1960  |144972.51|Structural Engineer         |                            |
  |2016-02-03 06:06:21|4  |Denise    |Riley    |driley3@gmpg.org        |Female|140.35.109.83  |3576031598965625   |China                 |4/8/1997  |90263.05 |Senior Cost Accountant      |                            |
  |2016-02-03 10:35:31|5  |Carlos    |Burns    |cburns4@miitbeian.gov.cn|      |169.113.235.40 |5602256255204850   |South Africa          |          |null     |                            |                            |
  |2016-02-03 12:52:34|6  |Kathryn   |White    |kwhite5@google.com      |Female|195.131.81.179 |3583136326049310   |Indonesia             |2/25/1983 |69227.11 |Account Executive           |                            |
  |2016-02-03 14:03:08|7  |Samuel    |Holmes   |sholmes6@foxnews.com    |Male  |232.234.81.197 |3582641366974690   |Portugal              |12/18/1987|14247.62 |Senior Financial Analyst    |                            |
  |2016-02-03 12:17:06|8  |Harry     |Howell   |hhowell7@eepurl.com     |Male  |91.235.51.73   |                   |Bosnia and Herzegovina|3/1/1962  |186469.43|Web Developer IV            |                            |
  |2016-02-03 09:22:53|9  |Jose      |Foster   |jfoster8@yelp.com       |Male  |132.31.53.61   |                   |South Korea           |3/27/1992 |231067.84|Software Test Engineer I    |1E+02                       |
  |2016-02-03 23:59:47|10 |Emily     |Stewart  |estewart9@opensource.org|Female|143.28.251.245 |3574254110301671   |Nigeria               |1/28/1997 |27234.28 |Health Coach IV             |                            |
  |2016-02-03 05:40:42|11 |Susan     |Perkins  |sperkinsa@patch.com     |Female|180.85.0.62    |3573823609854134   |Russia                |          |210001.95|                            |                            |
  |2016-02-03 23:34:34|12 |Alice     |Berry    |aberryb@wikipedia.org   |Female|246.225.12.189 |4917830851454417   |China                 |8/12/1968 |22944.53 |Quality Engineer            |                            |
  |2016-02-04 00:18:17|13 |Justin    |Berry    |jberryc@usatoday.com    |Male  |157.7.146.43   |6331109912871813274|Zambia                |8/15/1975 |44165.46 |Structural Analysis Engineer|                            |
  |2016-02-04 03:16:52|14 |Kathy     |Reynolds |kreynoldsd@redcross.org |Female|81.254.172.13  |5537178462965976   |Bosnia and Herzegovina|6/27/1970 |286592.99|Librarian                   |                            |
  |2016-02-03 14:23:23|15 |Dorothy   |Hudson   |dhudsone@blogger.com    |Female|8.59.7.0       |3542586858224170   |Japan                 |12/20/1989|157099.71|Nurse Practicioner          |<script>alert('hi')</script>|
  |2016-02-03 06:14:01|16 |Bruce     |Willis   |bwillisf@bluehost.com   |Male  |239.182.219.189|3573030625927601   |Brazil                |          |239100.65|                            |                            |
  |2016-02-03 06:27:45|17 |Emily     |Andrews  |eandrewsg@cornell.edu   |Female|29.231.180.172 |30271790537626     |Russia                |4/13/1990 |116800.65|Food Chemist                |                            |
  |2016-02-03 22:14:24|18 |Stephen   |Wallace  |swallaceh@netvibes.com  |Male  |152.49.213.62  |5433943468526428   |Ukraine               |1/15/1978 |248877.99|Account Representative I    |                            |
  |2016-02-03 17:15:54|19 |Clarence  |Lawson   |clawsoni@vkontakte.ru   |Male  |107.175.15.152 |3544052814080964   |Russia                |          |177122.99|                            |                            |
  |2016-02-03 16:00:36|20 |Rebecca   |Bell     |rbellj@bandcamp.com     |Female|172.215.104.127|                   |China                 |          |137251.19|                            |                            |
  +-------------------+---+----------+---------+------------------------+------+---------------+-------------------+----------------------+----------+---------+----------------------------+----------------------------+
  only showing top 20 rows

  root
   |-- registration_dttm: timestamp (nullable = true)
   |-- id: integer (nullable = true)
   |-- first_name: string (nullable = true)
   |-- last_name: string (nullable = true)
   |-- email: string (nullable = true)
   |-- gender: string (nullable = true)
   |-- ip_address: string (nullable = true)
   |-- cc: string (nullable = true)
   |-- country: string (nullable = true)
   |-- birthdate: string (nullable = true)
   |-- salary: double (nullable = true)
   |-- title: string (nullable = true)
   |-- comments: string (nullable = true)

   */
}
