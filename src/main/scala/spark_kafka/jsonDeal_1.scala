package spark_kafka

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructType, TimestampType}
import dao.deviceDate
import org.apache.spark.sql.functions._

object jsonDeal_1 {
  def main(args: Array[String]): Unit = {
    //引入sparkSession对象
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("jsonDEAL")
      .getOrCreate()
    //import implicit DF,DS
    import spark.implicits._
    //定义数据的要拆分的json结构
    val jsonSchema = new StructType()
      .add("battery_level", LongType)
      .add("c02_level", LongType)
      .add("cca3",StringType)
      .add("cn", StringType)
      .add("device_id", LongType)
      .add("device_type", StringType)
      .add("signal", LongType)
      .add("ip", StringType)
      .add("temp", LongType)
      .add("timestamp", TimestampType)
    //生成数据  转化为dataset类型
    val eventsDS =  Seq (
      (0, """{"device_id": 0, "device_type": "sensor-ipad", "ip": "68.161.225.1", "cca3": "USA", "cn": "United States", "temp": 25, "signal": 23, "battery_level": 8, "c02_level": 917, "timestamp" :1475600496 }"""),
      (1, """{"device_id": 1, "device_type": "sensor-igauge", "ip": "213.161.254.1", "cca3": "NOR", "cn": "Norway", "temp": 30, "signal": 18, "battery_level": 6, "c02_level": 1413, "timestamp" :1475600498 }"""),
      (2, """{"device_id": 2, "device_type": "sensor-ipad", "ip": "193.200.142.254", "cca3": "AUT", "cn": "Austria", "temp": 32, "signal": 27, "battery_level": 5, "c02_level": 1282, "timestamp" :1475600536 }"""))
      .toDF("id","device").as[deviceDate]
    //1.处理上面的json数据   -- from_json
    val devicesDF = eventsDS.select(from_json($"device",jsonSchema) as "device").select($"device.*")
      .filter($"device.device_id">1)


    //生成数据，保持dataframe类型即可
    val eventsDF =  Seq (
      (0, """{"device_id": 0, "device_type": "sensor-ipad", "ip": "68.161.225.1", "cca3": "USA", "cn": "United States", "temp": 25, "signal": 23, "battery_level": 8, "c02_level": 917, "timestamp" :1475600496 }"""),
      (1, """{"device_id": 1, "device_type": "sensor-igauge", "ip": "213.161.254.1", "cca3": "NOR", "cn": "Norway", "temp": 30, "signal": 18, "battery_level": 6, "c02_level": 1413, "timestamp" :1475600498 }"""),
      (2, """{"device_id": 2, "device_type": "sensor-ipad", "ip": "193.200.142.254", "cca3": "AUT", "cn": "Austria", "temp": 32, "signal": 27, "battery_level": 5, "c02_level": 1282, "timestamp" :1475600536 }"""))
      .toDF("id","device")
    //2.处理上面的数据   --get_json_object
    /**NOTE:注意：此方法使用的是JVM的堆外内存，所以在使用的时候需要调大堆外内存的大小。**/
    eventsDF.select($"id",get_json_object($"device","$.device_id"))


    //3.有时候需要将获取的数据转化为json格式     --to_json
    val jsonString = eventsDS.select(to_json(struct($"*"))).toDF("device")
    //jsonString.show(1)  //{"id":0,"device":...
    //eventsDS.show(1)    //0 | {"device_id": 0, ...
    //写入本地
    jsonString.write
      .mode("overwrite")
      .parquet("C:\\Users\\hazib\\Desktop\\git\\1")

    //从本地读取到spark
    val date = spark.read
      .parquet("C:\\Users\\hazib\\Desktop\\git\\1")
      .show()

    //4.将列转化为json格式的方法   --selectExpr
    val stringDF= eventsDS.selectExpr("cast (id as Int)","cast (device as String)")
    //stringDF.show(1)
  }
}
