package spark_kafka
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import dao.DeviceAlert

object jsonDeal_2 {
  def main(args: Array[String]): Unit = {

    //引入sparkSession对象
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("jsonDEAL")
      .getOrCreate()
    //import implicit DF,DS
    import spark.implicits._

    //定义一个复杂的数据类型
    val schema = new StructType()
      .add("dc_id", StringType)                               // data center where data was posted to Kafka cluster
      .add("source",                                          // info about the source of alarm
      MapType(                                              // define this as a Map(Key->value)
        StringType,
        new StructType()
          .add("description", StringType)
          .add("ip", StringType)
          .add("id", LongType)
          .add("temp", LongType)
          .add("c02_level", LongType)
          .add("geo",
            new StructType()
              .add("lat", DoubleType)
              .add("long", DoubleType)
          )
      )
    )
    //准备数据
    val dataDS = Seq("""
    {
      "dc_id": "dc-101",
      "source": {
        "sensor-igauge": {
        "id": 10,
        "ip": "68.28.91.22",
        "description": "Sensor attached to the container ceilings",
        "temp":35,
        "c02_level": 1475,
        "geo": {"lat":38.00, "long":97.00}
      },
        "sensor-ipad": {
        "id": 13,
        "ip": "67.185.72.1",
        "description": "Sensor ipad attached to carbon cylinders",
        "temp": 34,
        "c02_level": 1370,
        "geo": {"lat":47.41, "long":-122.00}
      },
        "sensor-inest": {
        "id": 8,
        "ip": "208.109.163.218",
        "description": "Sensor attached to the factory ceilings",
        "temp": 40,
        "c02_level": 1346,
        "geo": {"lat":33.61, "long":-111.89}
      }
      }
    }""").toDS()
    //print(dataDS.show)

    //处理数据
    val dataDF = spark.read.schema(schema).json(dataDS.rdd)
    val explodeDate = dataDF.select($"dc_id",explode($"source"))
    explodeDate.printSchema()
    //取数据
    explodeDate.select($"dc_id" as "dc_id",$"key" as "key",
      $"key" as "deviceType",
      $"value".getItem("ip").alias("ip"),
      $"value".getItem("id").alias("deviceId"),
      'value.getItem("geo").getItem("lat") as 'lat,                //note embedded level requires yet another level of fetching.
      'value.getItem("geo").getItem("long") as 'lon)
      .as[DeviceAlert]

  }
}
