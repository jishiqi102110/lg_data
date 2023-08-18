package warehouse.dwd.scada

import java.io.{BufferedInputStream, BufferedReader, FileInputStream, InputStream, InputStreamReader}
import java.net.URL

import breeze.linalg.split
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.break
import org.apache.spark.sql.functions._

object KylinCrucibleInfo {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .config("hive.exec.dynamici.partition", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .config("hive.warehouse.subdir.inherit.perms", "false")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .appName("app")
    .enableHiveSupport()
    .getOrCreate()

  // 读取csv文件数据
  def readCSVFile(path: String, spark: SparkSession): DataFrame = {
    val data = spark
      .read
      .option("inferSchema", "true")
      .option("sep", ",")
      .option("encoding", "utf-8") //gbk2312
      .option("header", false)
      .option("multiLine", "true")
      .csv(path).
      toDF() //  load(path)
    data
  }

  def main(args: Array[String]): Unit = {
    //val path1 = "file:///home/liangwt/file/data.csv"
    //    val month_id = "2023-02"
    //    val path1 = "E:\\Longi_files\\硅料项目(VIP)\\保山1130-211.csv"
    //    val path1 = "file:/C:/Users/mafangqiang/Desktop/银川和保山-2023.2.1-8.14-csv.csv"
    //val path1 = "file:/D:/data.csv"
    //D:\liangweitao2\Desktop\资料
    //val path = "E:\\Longi_files\\硅料项目(VIP)\\银川1130-214.csv"
    val path1 = "file:/E:/data.csv"
//    val resource: URL = this.getClass.getClassLoader.getResource("data.csv")
//    val in: InputStream = new FileInputStream(resource.getPath)
//    //    val bufferedInputStream = new BufferedInputStream(in)
//    //val input = KylinCrucibleInfo.getClass.getClassLoader.getResourceAsStream("/data.csv")
//    val inputStreamReader = new InputStreamReader(in, "UTF-8");
//    //        得到包装流
//    val bufferedReader = new BufferedReader(inputStreamReader);
//    //        读取数据
//    var line = bufferedReader.readLine()
//    //
//    val array = new ArrayBuffer[String]()
    //    // println("=============" + line)
    //
//    var flag = true
//    var count = 1
//    while (flag) {
//      if (count >= 22) {
//        array.append(line)
//      }
//      count += 1
//      line = bufferedReader.readLine();
//      if (line == null) {
//        flag = false
//      }
//    }
//    println("=============" + array.size)
    //    println("================" + array(0))
    //    println("================" + array(1))
    //    println("================" + array(2))
    //
    //    import spark.implicits._
    //    var df = spark.sparkContext.parallelize(array, 2).toDF("col")
    //
    //    val rdd = df.rdd.map{
    //      line=>
    //        val s= line.getAs[String](0)
    //        s.split(",")
    //    }
    //
    //
    //    println("++++++++++++++++++" + df.count())


    var cdf1 = readCSVFile(path1, spark)
    cdf1.show()
    println("***********************" + cdf1.count())
    cdf1 = cdf1.filter(col("_c1") !== "时间")
    println("***********************" + cdf1.count())
    cdf1.show(10)
    //    cdf1.createOrReplaceTempView("TEST")
    //    val test1 = spark.sql(
    //      """
    //        |SELECT count(*) from TEST
    //      """.stripMargin)
    //    test1.show(10)
    //    cdf1.show(10)
    //    print("***************************")
    ////    val path2 = "E:\\Longi_files\\硅料项目(VIP)\\银川1130-214.csv"
    //    val path2 = args(1)
    //    val cdf2 = readCSVFile(path2, spark)
    ////    cdf2.createOrReplaceTempView("TEST2")
    ////    val test2 = spark.sql(
    ////      """
    ////        |SELECT count(*) from TEST2
    ////      """.stripMargin)
    ////    test2.show(10)
    //
    //    val total_df = cdf1.union(cdf2)
    ////    total_df.createOrReplaceTempView("TEST3")
    ////    val test3 = spark.sql(
    ////      """
    ////        |SELECT count(*) from TEST3 where _c1 ='时间'
    ////      """.stripMargin)
    ////    test3.show(10)
    //
    //    total_df.createOrReplaceTempView("temp")
    //    val res_df = spark.sql(
    //      s"""
    //        |SELECT * FROM (
    //        |select *, left(_c0, 7) as month_id from temp
    //        |) A WHERE length(month_id)>6
    //      """.stripMargin)
    ////    res_df.show(10)
    //    res_df.createOrReplaceTempView("temp1")
    //    spark.sql(
    //      s"""
    //         |insert overwrite table dwd_pp_ingot.kylin_crucible_info2 partition(month_id)
    //         |select * from temp1
    //    """.stripMargin)
    //    print("*********** write successful ***********")

    //    val kylin_crucible_DF= spark.sql(
    //      """
    //        |select  * from dwd_pp_ingot.kylin_crucible_info2
    //        |""".stripMargin)
    //    println("********************"+kylin_crucible_DF.count())
    spark.stop()
  }
}
