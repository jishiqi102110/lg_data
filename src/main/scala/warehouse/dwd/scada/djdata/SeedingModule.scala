package warehouse.dwd.scada.djdata

import org.apache.spark.sql.SparkSession
import util.SparkUtil
/*
su - liangwt -c "spark-submit --class warehouse.dwd.scada.djdata.SeedingModule --master yarn --deploy-mode client --driver-memory 8g --num-executors 10 --executor-memory 8g --executor-cores 4 --queue spark --conf spark.shuffle.service.enabled=true --conf spark.sql.session.timeZone=Asia/Shanghai --conf spark.sql.catalogImplementation=hive --name SeedingModule --jars /home/liangwt/jar/hutool-core-5.8.18.jar,/home/liangwt/jar/hutool-json-5.8.18.jar /home/liangwt/jar/silicon_data_flow-1.0-SNAPSHOT-dependencies.jar"
 */

object SeedingModule {
  def main(args: Array[String]): Unit = {
//    val spark = SparkSession
//      .builder()
//      .master("local[6]")
//      .appName("SeedingModule")
//      .config("spark.debug.maxToStringFields", "1000")
//      .enableHiveSupport()
//      .getOrCreate()

    val spark =SparkUtil.hiveSparkSession("SeedingModule")


    val daysbefore = "2023-01-01"
    //    val daysbefore = args(0)
    val curday = "2023-08-10"
    //    val curday = args(1)
    val ingot_table = "ods_ingot_scada.djzk_app_result" //表名称
    // 重点代码1
    val ods_table = spark.sql(
      s"""
         |SELECT  basearea,          -- 基地
         |        crystalid,         -- 晶编
         |        nowtime,           -- 工步时间
         |        state,             -- 状态
         |        diameter,          -- 直径
         |        setdiameter,       -- 设定直径
         |        mainheater,        -- 主加功率
         |        setmainheater,     -- 设定主加功率
         |        bottomheater,      -- 底加功率
         |        setbottomheater,   -- 设定低加功率
         |        lag(setbottomheater, 1) over(partition BY basearea, crystalid order by nowtime) as last_sbheater,   --设定底加功率lag
         |        lag(setmainheater, 1) over(partition BY basearea, crystalid order by nowtime) as last_smheater,     --设定主加功率lag
         |        lag(mainheater, 1) over(partition BY basearea, crystalid order by nowtime) as last_mheater,         --设定主加功率lag
         |        lag(bottomheater, 1) over(partition BY basearea, crystalid order by nowtime) as last_bheater,       --设定主加功率lag
         |        lag(nowtime, 1) over(partition BY basearea, crystalid order by nowtime) as lasttime,
         |        lag(state, 1) over(partition by basearea, crystalid order by nowtime) as laststate,
         |        lead(state, 1) over(partition by basearea, crystalid order by nowtime) as leadstate,
         |        avgslspeed,                                                                                         --平均拉速
         |        setslspeed,                                                                                         --设定拉速
         |        meltsurftemp,                                                                                       --液温
         |        setmeltsurftemp,                                                                                    --设定液温
         |        meltlevel,                                                                                          --液口距
         |        setmeltlevel,                                                                                       --设定液口距
         |        seedlift,                                                                                           --晶升
         |        cruciblelift,                                                                                       --埚升
         |        seedrotation,                                                                                       --晶转
         |        cruciblerotation,                                                                                   --埚转
         |        argonflow,                                                                                          --氩气流量
         |        mainpressure100t,                                                                                   --主室压力
         |        crystallength,                                                                                      --晶长
         |        cruciblepos,                                                                                        --埚位
         |        crystalpos,                                                                                         --晶位
         |        crystalweight,                                                                                      --晶重
         |        remainweight,                                                                                       --剩料量
         |        pumpfrequency,                                                                                      --干泵频率
         |        mainheatercurrent,                                                                                  --主加电流
         |        bottomheatercurrent,                                                                                --底加电流
         |        heatexchangerpos,                                                                                   --换热器位置
         |        setpumpfrequency,                                                                                   --设定干泵频率
         |        feedervalveclose,                                                                                   --闸阀关
         |        feedervalveopen,                                                                                    --闸阀开
         |        isolationvalveopen,                                                                                 --隔离阀开
         |        isolationvalveclose,                                                                                --设定主加功率lag
         |        case when ((state > 4) and (state <= 9)) then state else 0 end fill_state
         | FROM ${ingot_table}
         |WHERE 1=1
         |  AND day_id >= '${daysbefore}'
         |  AND day_id <= '${curday}'
      """.stripMargin)
    //      .filter("crystalid='BBS38B5401'")

    // 表重命名
    ods_table.createOrReplaceTempView("ingot_result")

    // 重点代码2--RCZ分组
    val ods_spark_df = spark.sql(
      s"""
         |
         |SELECT *
         |    , SUM(XISOPENBOT) OVER(PARTITION BY basearea,crystalid ORDER BY NOWTIME) XBGROUP   -- RCZ
         |  FROM ( SELECT *
         |              ,SUBSTR(BASEAREA, -3) as puller    --炉号
         |              ,CASE WHEN ((last_sbheater + last_smheater < 130)
         |                  and (setbottomheater + setmainheater >= 130)) then 1 else 0 end xisopenbot -- 是否开底加
         |         FROM ingot_result
         |         )
      """.stripMargin)
    ods_spark_df.createOrReplaceTempView("ods_table")
    // 重点代码3--RCZ错误判断修正
    val group_df = spark.sql(
      s"""
         |SELECT *,
         |        SUM(ISOPENBOT) OVER(partition by basearea, crystalid order by nowtime) RCZ     ---- RCZ段数
         | FROM (
         |          SELECT *,
         |                 CASE WHEN (MAXSTATE IN (4, 5, 6, 7, 8, 9)) THEN XISOPENBOT ELSE 0 END ISOPENBOT
         |          FROM (
         |                   SELECT *,
         |                          MAX(fill_state) OVER(PARTITION BY basearea, crystalid,XBGROUP) MAXSTATE
         |                   FROM ods_table
         |               ) ALAYER
         |      ) BLAYER
      """.stripMargin)
      .where("RCZ>0")
      .filter("state in (1,4,5,6)")
      .createOrReplaceTempView("rcz_group_result_temp")

    // 去掉获取数据中第一个晶遍的数据，因为第一个晶遍数据可能不全
    spark.sql(
      """
        |     SELECT * FROM
        |     (SELECT t.*,
        |            row_number() over(partition by basearea order by crystalid) as rank_num
        |       FROM rcz_group_result_temp t
        |      )
        |      WHERE rank_num > 0
        |""".stripMargin)
      .createOrReplaceTempView("rcz_group_result")

    // 放肩最近一次的引晶分组完成
    spark.sql(
      """
        | SELECT basearea,
        |        nowtime,
        |        state,
        |        laststate,
        |        leadstate,
        |        crystalid,
        |        rcz,
        |        seeding_group,
        |        crystallen_below80_group,
        |        crystallen_over80_group,
        |        crystallen_over100_group,
        |        crystallen_over130_group,
        |        crystallen_over160_group,
        |        max(leadstate) over (PARTITION BY basearea, crystalid,rcz,seeding_group) as max_state_of_seedgroup,
        |        diameter,
        |        avgslspeed,
        |        meltsurftemp,
        |        setmeltsurftemp,
        |        crystallength
        |   FROM (SELECT basearea,
        |                nowtime,
        |                state,
        |                laststate,
        |                leadstate,
        |                crystalid,
        |                rcz,
        |                diameter,
        |                avgslspeed,
        |                meltsurftemp,
        |                setmeltsurftemp,
        |                crystallength,
        |                sum(case
        |                      when laststate = 4 and state = 5 then 1
        |                      else 0
        |                     end) over (PARTITION BY basearea, crystalid,rcz order by nowtime) seeding_group,
        |                case when crystallength < 80 then 1 else 0 end as   crystallen_below80_group,
        |                case when crystallength >= 80 then 1 else 0 end as  crystallen_over80_group,
        |                case when crystallength >= 100 then 1 else 0 end as crystallen_over100_group,
        |                case when crystallength >= 130 then 1 else 0 end as crystallen_over130_group,
        |                case when crystallength >= 160 then 1 else 0 end as crystallen_over160_group
        |           FROM rcz_group_result
        |        ) t
        |  WHERE t.state=5
        |""".stripMargin)
      .filter("max_state_of_seedgroup=6") // 6为放肩，取出所有放肩前最近一次引晶的数据
      .createOrReplaceTempView("seeding_result")

    // 结果中间表，分子分母处理完成
    spark.sql(
      """
        |     SELECT  basearea,
        |             nowtime,
        |             crystalid,
        |             rcz,
        |             state,
        |             diameter,
        |             avgslspeed,
        |             meltsurftemp,
        |             setmeltsurftemp,
        |             crystallength,
        |             seeding_group,
        |             min(diameter) over(partition by basearea,crystalid,rcz,seeding_group) as min_diameter,    -- 最小直径
        |             max(diameter) over(partition by basearea,crystalid,rcz,seeding_group) as max_diameter,    -- 最大直径
        |             min(avgslspeed) over(partition by basearea,crystalid,rcz,seeding_group) as min_avgslspeed,  -- 最小拉速
        |             max(avgslspeed) over(partition by basearea,crystalid,rcz,seeding_group) as max_avgslspeed,  -- 最大拉速
        |             first_value(meltsurftemp) over(partition by basearea,crystalid,rcz,seeding_group order by nowtime) as start_meltsurftemp,      -- 引晶开始液面亮度
        |             first_value(meltsurftemp) over(partition by basearea,crystalid,rcz,seeding_group order by nowtime desc) as end_meltsurftemp,  -- 引晶结束液面亮度
        |             first_value(crystallength) over(partition by basearea,crystalid,rcz,seeding_group order by nowtime) as start_crystallength,     -- 引晶开始晶体长度
        |             first_value(crystallength) over(partition by basearea,crystalid,rcz,seeding_group order by nowtime desc) as end_crystallength, -- 引晶结束晶体长度
        |             first_value(nowtime) over(partition by basearea,crystalid,rcz,seeding_group order by nowtime) as start_nowtime,     -- 引晶开始晶体长度
        |             first_value(nowtime) over(partition by basearea,crystalid,rcz,seeding_group order by nowtime desc) as end_nowtime, -- 引晶结束晶体长度
        |             case when crystallen_below80_group=1 then min(crystallength) over(partition by basearea,crystalid,rcz,seeding_group,crystallen_below80_group order by nowtime) else null end as min_crystallength_below80,       -- 晶体最小长度(晶体长度0-80mm分组)
        |             case when crystallen_below80_group=1 then max(crystallength) over(partition by basearea,crystalid,rcz,seeding_group,crystallen_below80_group order by nowtime desc) else null end as max_crystallength_below80,  -- 晶体最大长度(晶体长度0-80mm分组)
        |             case when crystallen_below80_group=1 then first_value (nowtime) over(partition by basearea,crystalid,rcz,seeding_group,crystallen_below80_group order by nowtime ) else null end as min_nowtime_below80,         -- 引晶开始时间(晶体长度0-80mm分组)
        |             case when crystallen_below80_group=1 then first_value(nowtime) over(partition by basearea,crystalid,rcz,seeding_group,crystallen_below80_group order by nowtime desc) else null end as max_nowtime_below80,      -- 引晶结束时间(晶体长度0-80mm分组)
        |
        |             case when crystallen_over80_group=1 then min(crystallength) over(partition by basearea,crystalid,rcz,seeding_group,crystallen_over80_group order by nowtime) else null end as min_crystallength_over80,          -- 晶体最小长度(晶体长度大于等于80分组)
        |             case when crystallen_over80_group=1 then max(crystallength) over(partition by basearea,crystalid,rcz,seeding_group,crystallen_over80_group order by nowtime desc) else null end as max_crystallength_over80,     -- 晶体最大长度(晶体长度大于等于80分组)
        |             case when crystallen_over80_group=1 then first_value (nowtime) over(partition by basearea,crystalid,rcz,seeding_group,crystallen_over80_group order by nowtime ) else null end as min_nowtime_over80,            -- 引晶开始时间(晶体长度大于等于80分组)
        |             case when crystallen_over80_group=1 then first_value(nowtime) over(partition by basearea,crystalid,rcz,seeding_group,crystallen_over80_group order by nowtime desc) else null end as max_nowtime_over80,         -- 引晶结束时间(晶体长度大于等于80分组)
        |
        |             case when crystallen_over100_group=1 then min(crystallength) over(partition by basearea,crystalid,rcz,seeding_group,crystallen_over100_group order by nowtime) else null end as min_crystallength_over100,       -- 晶体最小长度(晶体长度大于等于100mm分组)
        |             case when crystallen_over100_group=1 then max(crystallength) over(partition by basearea,crystalid,rcz,seeding_group,crystallen_over100_group order by nowtime desc) else null end as max_crystallength_over100,  -- 晶体最大长度(晶体长度大于等于100mm分组)
        |             case when crystallen_over100_group=1 then first_value (nowtime) over(partition by basearea,crystalid,rcz,seeding_group,crystallen_over100_group order by nowtime ) else null end as min_nowtime_over100,         -- 引晶开始时间(晶体长度大于等于100mm分组)
        |             case when crystallen_over100_group=1 then first_value(nowtime) over(partition by basearea,crystalid,rcz,seeding_group,crystallen_over100_group order by nowtime desc) else null end as max_nowtime_over100,      -- 引晶结束时间(晶体长度大于等于100mm分组)
        |
        |             case when crystallen_over130_group=1 then min(crystallength) over(partition by basearea,crystalid,rcz,seeding_group,crystallen_over130_group order by nowtime) else null end as min_crystallength_over130,       -- 晶体最小长度(晶体长度大于等于130mm分组)
        |             case when crystallen_over130_group=1 then max(crystallength) over(partition by basearea,crystalid,rcz,seeding_group,crystallen_over130_group order by nowtime desc) else null end as max_crystallength_over130,  -- 晶体最大长度(晶体长度大于等于130mm分组)
        |             case when crystallen_over130_group=1 then first_value (nowtime) over(partition by basearea,crystalid,rcz,seeding_group,crystallen_over130_group order by nowtime ) else null end as min_nowtime_over130,         -- 引晶开始时间(晶体长度大于等于130mm分组)
        |             case when crystallen_over130_group=1 then first_value(nowtime) over(partition by basearea,crystalid,rcz,seeding_group,crystallen_over130_group order by nowtime desc) else null end as max_nowtime_over130,      -- 引晶结束时间(晶体长度大于等于130mm分组)
        |
        |             case when crystallen_over160_group=1 then min(crystallength) over(partition by basearea,crystalid,rcz,seeding_group,crystallen_over160_group order by nowtime) else null end as min_crystallength_over160,       -- 晶体最小长度(晶体长度大于等于160mm分组)
        |             case when crystallen_over160_group=1 then max(crystallength) over(partition by basearea,crystalid,rcz,seeding_group,crystallen_over160_group order by nowtime desc) else null end as max_crystallength_over160,  -- 晶体最大长度(晶体长度大于等于160mm分组)
        |             case when crystallen_over160_group=1 then first_value (nowtime) over(partition by basearea,crystalid,rcz,seeding_group,crystallen_over160_group order by nowtime ) else null end as min_nowtime_over160,         -- 引晶开始时间(晶体长度大于等于160mm分组)
        |             case when crystallen_over160_group=1 then first_value(nowtime) over(partition by basearea,crystalid,rcz,seeding_group,crystallen_over160_group order by nowtime desc) else null end as max_nowtime_over160       -- 引晶结束时间(晶体长度大于等于160mm分组)
        |       FROM seeding_result
        |""".stripMargin)
      .createOrReplaceTempView("result_temp")

    spark.sql(
      """
        |   SELECT basearea,
        |          crystalid,
        |          rcz,
        |          seeding_group,
        |          max(start_nowtime) as start_nowtime,   -- 引晶开始时间
        |          max(end_nowtime)  as end_nowtime,      -- 引晶结束时间
        |          max(min_diameter) as min_diameter,
        |          max(max_diameter) as max_diameter,
        |          max(min_avgslspeed) as min_avgslspeed,
        |          max(max_avgslspeed) as max_avgslspeed,
        |          (max(start_meltsurftemp) - max(setmeltsurftemp))  as start_diff_set_meltsurftemp,
        |          (max(start_meltsurftemp) - max(end_meltsurftemp))  as start_diff_end_meltsurftemp,
        |          (max(end_meltsurftemp) - max(setmeltsurftemp))  as end_diff_set_meltsurftemp,
        |          ((max(end_crystallength) - max(start_crystallength)) / (unix_timestamp(max(end_nowtime))-unix_timestamp(max(start_nowtime)))) * 3600 as avg_avgslspeed,
        |          ((max(max_crystallength_below80) - max(min_crystallength_below80)) / (unix_timestamp(max(max_nowtime_below80))-unix_timestamp(max(min_nowtime_below80)))) * 3600 as below80_avg_slspeed,
        |          ((max(max_crystallength_over80) -  max(min_crystallength_over80))  / (unix_timestamp(max(max_nowtime_over80)) -unix_timestamp(max(min_nowtime_over80)))) * 3600 as over80_avg_slspeed,
        |          ((max(max_crystallength_over100) - max(min_crystallength_over100)) / (unix_timestamp(max(max_nowtime_over100))-unix_timestamp(max(min_nowtime_over100)))) * 3600 as over100_avg_slspeed,
        |          ((max(max_crystallength_over130) - max(min_crystallength_over130)) / (unix_timestamp(max(max_nowtime_over130))-unix_timestamp(max(min_nowtime_over130)))) * 3600 as over130_avg_slspeed,
        |          ((max(max_crystallength_over160) - max(min_crystallength_over160)) / (unix_timestamp(max(max_nowtime_over160))-unix_timestamp(max(min_nowtime_over160))))  * 3600 as over160_avg_slspeed
        |     FROM result_temp
        |    GROUP BY basearea,crystalid,rcz,seeding_group
        |""".stripMargin)
      .createOrReplaceTempView("result_view")

    spark.sql(
      s"""
         |INSERT OVERWRITE table test.test_seeding_indicator
         |SELECT * FROM result_view
  """.stripMargin)
    spark.stop()

    // 5: 引晶  6: 放肩
  }

}
