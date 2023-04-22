package org.rubigdata

import org.apache.hadoop.io.NullWritable
import de.l3s.concatgz.io.warc.{WarcGzInputFormat,WarcWritable}
import de.l3s.concatgz.data.WarcRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.commons.lang3.StringUtils
import org.jsoup.Jsoup
import org.jsoup.nodes.{Document,Element}
import org.apache.spark.sql.functions.udf

object RUBigDataApp {
  def main(args: Array[String]) {

    val warcfile = s"hdfs:///cc-crawl/segments/1618038060927.2/warc/CC-MAIN-20210411030031-20210411060031-00586.warc.gz"

    val sparkConf = new SparkConf()
                      .setAppName("RUBigData WARC4Spark 2020")
                      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                      .registerKryoClasses(Array(classOf[WarcRecord]))
                      .set("spark.dynamicAllocation.enabled", "true")
		      .set("spark.shuffle.service.enabled", "true")
                      //.set("spark.driver.memory", "4g")
                      //.set("spark.executor.memory", "8g")
    implicit val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    val warcs = sc.newAPIHadoopFile(
              warcfile,
              classOf[WarcGzInputFormat],             // InputFormat
              classOf[NullWritable],                  // Key
              classOf[WarcWritable]                   // Value>
    )

    val clean =
     warcs.map{ wr => wr._2 }.
        filter{ _.isValid() }.
        map{ _.getRecord() }.
        filter{_.getHeader().getHeaderValue("WARC-Type") == "response" }.
        map{warc => {
            val url = warc.getHeader().getUrl()
            (warc, url)
            }
        }.
        filter{x => x._2.contains("bbc.com") || x._2.contains("wsj.com") ||x._2.contains("nytimes.com")}.
        map{wh => (wh._1.getHttpStringBody(), wh._2)}.
        filter{ _._1.length > 0 }.
        map{body => (Jsoup.parse(body._1), body._2)}.
        map{ js => {
                        val t = js._1.title()
                        val link =js._2
                        val time = js._1.select("time").attr("datetime") //for selecting and filtering time
                        (time, t, link)
                    }
            }.
        filter {x => x._1.length>0 && x._2.length>0 && x._3.length>0}


    val webDF =clean.toDF("time", "title", "link")
    webDF.createOrReplaceTempView("web")

    val filtered = spark.sql(
    	"""
    	SELECT time, title, link
    	FROM web
    	WHERE time LIKE '%2020%'
    	AND title LIKE '%Bitcoin%'
    	""".stripMargin)
    filtered.createOrReplaceTempView("filtered");

    object timeConverter extends Serializable{

    	def toMonth(t:String): String =
    	{
        	if(t.length>=7  && !t.toLowerCase().contains("a") && !t.toLowerCase().contains("e") && !t.toLowerCase().contains("u")){
            		val month = s"${t(5)}${t(6)}"
            		month
		}
        	else{
           		val month = "0"
            		month
       	 	}
    	}
   	val toMonthUDF = udf((f: String) => toMonth(f))
   }
   val converted = filtered.withColumn("month", timeConverter.toMonthUDF($"time"))
   converted.createOrReplaceTempView("converted");

   val grouped = spark.sql(
   	 """
   	 SELECT month, Count(title) as frequency
   	 FROM converted
   	 WHERE month <> '0'
   	 GROUP BY month
   	 ORDER BY month ASC
   	 """.stripMargin)

   grouped.write.csv("hdfs:///user/s1034535/result")

  }
}
