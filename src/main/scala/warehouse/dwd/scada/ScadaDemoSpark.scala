package warehouse.dwd.scada

package com.longi_silicon.scada

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import org.apache.spark.sql.SparkSession
/*
  此任务为测试任务，主要测试spark应用开发及线上环境数据读取测试
 */
object ScadaDemoSpark {
  def main(args: Array[String]): Unit = {

    val prop: Properties = new Properties()

    val spark = SparkSession.builder()
      .appName("ScadaDemoSpark")
      .config("hive.exec.dynamici.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      //.config("hive.warehouse.subdir.inherit.perms", "false")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    // 南郊集群
    /////生产环境
    val kuduMaster = "worker01.longi:7051,worker02.longi:7051,worker03.longi:7051"
    // 北郊集群
    //val kuduMaster = "worker01.center.longi:7051,worker03.center.longi:7051,worker05.center.longi:7051"
    /////到此结束以上01------以上为公共程序，不用修改，直接引用；
    import spark.implicits._

    //step1 测试本地spark
    val arr = Array(("id1", 2, "f1"), ("id2", 1, "f2"), ("id3", 3, "f3"))
    val df = spark.sparkContext.parallelize(arr).toDF("id", "intValue", "stringValue")
    df.show()

    //step2 测试读取线上hive表
    val db="ods_ingot_scada"

    //val kuduPrefix="streaming_"
    val kuduPrefix=""

    val table ="djzk_app_result"
    val kuduTableStr=db+"."+kuduPrefix+table
    // todo 临时表修改
    val outTable = "dwd_pp_ingot.ingot_djzk_analysis_result"
    // 等径数据
    // todo 修改读取数据源
    //    val dj_DF = spark.read.format("kudu").options(
    //      Map("kudu.master" -> kuduMaster, "kudu.table" -> kuduTableStr))
    //      .load().where("state in (7, 8, 1, 9, 20, 17,0,26) " +
    //      " and substring(crystalid,2,2) in " +
    //      "('GN', 'GP','NN', 'NP','TN', 'TP','BN', 'BP','EN', 'EP','LN', 'LP','HN', 'HP','YN', 'YP','QN', 'QP')" +
    //      " and length(crystalid)=10 and  crystallength > 0")

    var dj_DF= spark.sql(
      """
        |SELECT *, max(dj_end_time) OVER(PARTITION BY basearea,crystalid, dj_group) AS max_dj_end_time
        |FROM (
        |         SELECT *
        |              , max(lead_state) OVER(PARTITION BY basearea,crystalid, dj_group) AS max_lead_state
        |              , min(nowtime) OVER(PARTITION BY basearea,crystalid, dj_group) AS  dj_start_time
        |              , CASE WHEN lead_state!=8 and lead_state is not null THEN nowtime else null END as dj_end_time
        |              , left (nowtime,10) as day_id
        |         FROM (
        |             SELECT base
        |                 , substring (basearea, 4, 1) as area
        |                 , right (basearea
        |                 , 3) as puller
        |                 , basearea
        |                 , nowtime
        |                 , crystalid
        |                 , crystalid_11
        |                 , crystallength
        |                 , state
        |                 , lag_state
        |                 , lead_state
        |                 , avgslspeed
        |                 , SUM (is_dj) OVER(PARTITION BY basearea
        |                 , crystalid ORDER BY NOWTIME) AS dj_group                                            --- 等径分组
        |                 , diameter
        |                 , mainheater
        |                 , setmainheater
        |                 , bottomheater
        |                 , setbottomheater
        |                 , setslspeed
        |                 , meltsurftemp
        |                 , setmeltsurftemp
        |                 , meltlevel
        |                 , setmeltlevel
        |                 , cruciblelift
        |                 , seedrotation
        |                 , cruciblerotation
        |                 , argonflow
        |                 , mainpressure100t
        |                 , cruciblepos
        |                 , remainweight
        |                 , pumpfrequency
        |                 , crystalid6
        |                 , heatexchangerpos
        |             FROM (
        |             SELECT *
        |                 , CASE WHEN state =8 AND lag_state=7 THEN 1 ELSE 0 END AS is_dj                      --- 大等径分组
        |             FROM (
        |             SELECT basearea
        |                 , CASE WHEN RIGHT (LEFT (basearea, 3), 1)='7' THEN 'Y7'
        |             WHEN RIGHT (LEFT (basearea, 3), 1)='1' THEN 'Y1'
        |             WHEN RIGHT (LEFT (basearea, 3), 1)='2' THEN 'H2'
        |             ELSE LEFT (basearea, 2) END AS base
        |                 , nowtime
        |                 , LAG(nowtime, 1) OVER(PARTITION BY basearea, crystalid ORDER BY NOWTIME) AS LAST_NT ---- 时间下移一位
        |                 , crystalid
        |                 , crystalid6
        |                 , concat(crystalid, crystalid6) as crystalid_11
        |                 , state
        |                 , LAG(state, 1) OVER(PARTITION BY basearea, crystalid ORDER BY NOWTIME) AS lag_state
        |                 , LEAD(state, 1) OVER(PARTITION BY basearea, crystalid ORDER BY NOWTIME) AS lead_state
        |                 , crystallength
        |                 , avgslspeed
        |                 , diameter
        |                 , mainheater
        |                 , setmainheater
        |                 , bottomheater
        |                 , setbottomheater
        |                 , setslspeed
        |                 , meltsurftemp
        |                 , setmeltsurftemp
        |                 , meltlevel
        |                 , setmeltlevel
        |                 , cruciblelift
        |                 , seedrotation
        |                 , cruciblerotation
        |                 , argonflow
        |                 , mainpressure100t
        |                 , cruciblepos
        |                 , remainweight
        |                 , pumpfrequency
        |                 , heatexchangerpos
        |             FROM ods_ingot_scada.djzk_app_result
        |             WHERE length (crystalid)=10
        |             and crystallength > 0
        |             and day_id >= '2023-05-01'
        |             and state in (7, 8, 1, 9, 20, 17, 0, 26)
        |             and (substring (crystalid, 2, 2) in ('GN', 'GP',
        |             'NN', 'NP',
        |             'TN', 'TP',
        |             'BN', 'BP',
        |             'EN', 'EP',
        |             'LN', 'LP',
        |             'HN', 'HP',
        |             'YN', 'YP',
        |             'QN', 'QP')
        |             or
        |             substring (basearea, 4, 1) in ('E', 'D', 'F')
        |             )
        |
        |             ) ALAYER where state = 8
        |             ) BLAYER
        |             ) CLAYER
        |         WHERE dj_group > 0
        |     ) DLAYER
        |where max_lead_state != 20
        |order by dj_group, nowtime
        |""".stripMargin)

    dj_DF.createTempView("dj_df")
    println("***************************************")
    println("step1 等径原始kudu表数据")
    dj_DF.show(100)
    println("***************************************")


    //    val sql =
    //  s"""
    //        |SELECT *
    //        |FROM (
    //        |         SELECT *
    //        |              , max(lead_state) OVER(PARTITION BY basearea,crystalid, dj_group) AS max_lead_state
    //        |              , min(nowtime) OVER(PARTITION BY basearea,crystalid, dj_group) AS  dj_start_time
    //        |              , CASE WHEN lead_state!=8 and lead_state is not null THEN nowtime else null END as dj_end_time
    //        |              ,left (nowtime,10) as day_id
    //        |         FROM (
    //        |                  SELECT base
    //        |                       , substring(basearea, 4, 1) as area
    //        |                       , right (basearea
    //        |                       , 3) as puller
    //        |                       , basearea
    //        |                       , nowtime
    //        |                       , crystalid
    //        |                       , crystalid_11
    //        |                       , crystallength
    //        |                       , state
    //        |                       , lag_state
    //        |                       , lead_state
    //        |                       , avgslspeed
    //        |                       , SUM (is_dj) OVER(PARTITION BY basearea
    //        |                       , crystalid ORDER BY NOWTIME) AS dj_group --- 等径分组
    //        |                  FROM (
    //        |                      SELECT *
    //        |                          , CASE WHEN state =8 AND lag_state=7 THEN 1 ELSE 0 END AS is_dj                      --- 大等径分组
    //        |                      FROM (
    //        |                      SELECT basearea
    //        |                          , CASE WHEN RIGHT(LEFT (basearea, 3), 1)='7' THEN 'Y7'
    //        |                                          WHEN RIGHT (LEFT (basearea, 3), 1)='1' THEN 'Y1'
    //        |                                          WHEN RIGHT (LEFT (basearea, 3), 1)='2' THEN 'H2'
    //        |                                         ELSE LEFT (basearea, 2) END AS base
    //        |                          , nowtime
    //        |                          , LAG(nowtime, 1) OVER(PARTITION BY basearea, crystalid ORDER BY NOWTIME) AS LAST_NT ---- 时间下移一位
    //        |                          , crystalid
    //        |                          , crystalid6
    //        |                          , concat(crystalid, crystalid6) as crystalid_11
    //        |                          , state
    //        |                          , LAG(state, 1) OVER(PARTITION BY basearea, crystalid ORDER BY NOWTIME) AS lag_state
    //        |                          , LEAD(state, 1) OVER(PARTITION BY basearea, crystalid ORDER BY NOWTIME) AS lead_state
    //        |                          , crystallength
    //        |                          , avgslspeed
    //        |                      FROM dj_df
    //        |                      WHERE length (crystalid)=10
    //        |                      and crystallength > 0
    //        |                      and state in (7, 8, 1, 9, 20, 17,0,26)
    //        |                      and substring(crystalid,2,2) in ('GN', 'GP',
    //        |                                                              'NN', 'NP',
    //        |                                                              'TN', 'TP',
    //        |                                                            'BN', 'BP',
    //        |                                                            'EN', 'EP',
    //        |                                                            'LN', 'LP',
    //        |                                                            'HN', 'HP',
    //        |                                                            'YN', 'YP',
    //        |                                                            'QN', 'QP')
    //        |                      ) ALAYER where state = 8
    //        |                      ) BLAYER
    //        |              ) CLAYER
    //        |         WHERE dj_group > 0
    //        |     ) DLAYER where max_lead_state != 20
    //        |order by dj_group, nowtime
    //        |""".stripMargin
    //    var result = spark.sql(sql)

    println("***************************************")
    println("step2 结果表数据")
    println("***************************************")
    // todo 分区
    dj_DF = dj_DF.withColumn("dj_end_time",$"max_dj_end_time").drop("max_dj_end_time")
    //.repartition(5)
    dj_DF.show(100)
    dj_DF.createTempView("result_df")
    println("step3 写入结果表数据")

    //除了第一次导入数据，后续数据可以每次只获取3天前的数据入库即可，然后覆盖就行
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE,-3)
    val day: String = dateFormat.format(cal.getTime)

    spark.sql(
      s"""
         |insert overwrite table $outTable partition(day_id)
         |select * from result_df
      """.stripMargin)
    //where day_id > $day
    println("***** 成功写入 ******")

    spark.stop()

  }
}

