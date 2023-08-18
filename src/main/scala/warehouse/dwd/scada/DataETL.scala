package warehouse.dwd.scada

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import warehouse.dwd.scada.KylinCrucibleInfo.{readCSVFile, spark}
/*
此工具为银川保山数据同步工具
su - liangwt -c "spark-submit --class warehouse.dwd.scada.DataETL --master local[2] --deploy-mode client --driver-memory 6g --executor-cores 2 --queue spark --conf spark.shuffle.service.enabled=true --conf spark.sql.session.timeZone=Asia/Shanghai --conf spark.sql.catalogImplementation=hive --name KylinCrucibleInfo /home/liangwt/jar/silicon_data_flow-1.0-SNAPSHOT-dependencies.jar"

 */

object DataETL {
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
    val path1 = "file:///home/liangwt/file/data.csv"
    //val path1 = "file:/E:/data.csv"

    var df = readCSVFile(path1, spark)
    println("***********************" + df.count())
    df.show()
   //df = df.filter(col("_c1") !== "时间" &&& col("_c2") !== "班次" )

    df.show()
    println("***********************" + df.count())


    df.createTempView("df")
    spark.sql(
      s"""
         |insert OVERWRITE table dwd_pp_ingot.ingot_crucible_info_prd partition(month_id)
         |select *, left(_c0, 7) as month_id from df
        """.stripMargin)
    spark.stop()

  }

}
