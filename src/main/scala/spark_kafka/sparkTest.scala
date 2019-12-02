package spark_kafka
import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession


object sparkTest {
  def main(args: Array[String]): Unit = {
    //创建sparksession对象
    val spark = SparkSession
      .builder
      .appName("structedNetworkWordCount")
      .master("local[1]")
      .getOrCreate()
    //创建streaming读取的对象
    //导入spark的隐式转化
    import spark.implicits._
    val lines = spark.readStream
      .format("text")
      .load("D:\\360Downloads\\1\\")
    //进行切割数据
    val words = lines.as[String].flatMap(_.split(" "))
    //计算wordcount
    val res = words.groupBy("value").count()
    /*
    //输出为文件格式
    val query = res.writeStream
      .outputMode("append")
      .format("parquet")
      .option("checkpointLocation", "D:\\360Downloads\\checkpoint")
      .option("path","D:\\360Downloads\\1res")
      .start()
    //启动流计算
    */

    //输出到控制台中
    val query = res.writeStream
      .outputMode("complete")
      .format("console")
      .queryName("helloworld")
      .option("checkpointLocation", "D:\\360Downloads\\1res")
      .start()
    query.awaitTermination()
    println(query.lastProgress)

  }
}
