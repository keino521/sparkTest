import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator


object test {
  def main(args: Array[String]): Unit = {
    //
    val sc = SparkSession.builder()
      .appName("testRDD")
      .master("local[1]")
      .getOrCreate()
      .sparkContext

        var t2 = sc.textFile("C:\\Users\\hazib\\Desktop\\1.txt")
    var t = sc.parallelize(Array("521"))
    //    println(t2.count())
    //
    //    val t = spark.sparkContext.parallelize(Array("1","2","3","hazi"))
    //
    //t.cache()
    //可以输出当前对象或者当前的RDD缓存的大小，默认大小为字节   用来进行有效的内存管理
    //    println(SizeEstimator.estimate(t))
    //

    t.persist(StorageLevel.MEMORY_AND_DISK_2)
    t2.persist(StorageLevel.MEMORY_AND_DISK_2)
    //    t2.persist(StorageLevel.MEMORY_ONLY)
    println(SizeEstimator.estimate(t))
    println(sc.uiWebUrl)
    Thread.sleep(360000000)
  }
}
