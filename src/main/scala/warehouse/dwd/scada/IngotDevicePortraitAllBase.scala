package warehouse.dwd.scada

import org.apache.spark.sql.SparkSession

object IngotDevicePortraitAllBase {
  def main(args: Array[String]): Unit = {
    //北郊集群
    //        val kuduMaster = "worker01.center.longi:7051,worker03.center.longi:7051,worker05.center.longi:7051"
    // 南郊集群worker01.longi:7051,worker02.longi:7051,worker03.longi:7051
    //    val kuduMaster = "worker01.longi:7051,worker02.longi:7051,worker03.longi:7051"
    // 定义表名称
    //     val djzkResModel = "ods_ingot_scada.streaming_djzk_app_result"
    val djzkResModel = "ods_ingot_scada.djzk_app_result"

    //    System.setProperty("user.name", "mafq")
    //    System.setProperty("HADOOP_USER_NAME", "mafq")
    // 1. 初始化kudu-saprk
    val spark = SparkSession
      .builder()
      .appName("IngotDevicePortrait")
      //      .master("local[*]")
      .config("hive.exec.dynamic.partition", true)
      .config("spark.sql.parquet.writeLegacyFormat", true)
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      // .config("hive.warehouse.subdir.inherit.perms", "false")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // .config("spark.sql.warehouse.dir", "file:///D:/sparkproject/workspace/spark-warehouse") // 本机使用
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    val daysbefore = args(0)
    val curday = args(1)
    //    val daysbefore = "2023-01-01"
    //    val curday = "2023-01-07"

    // 2.读取kudu表数据
    //        spark.read.format("kudu").options(
    //          Map("kudu.master"->kuduMaster, "kudu.table"->djzkResModel)
    //        ).load().createOrReplaceTempView("streaming_djzk_app_result")

    val ods_table = spark.sql(
      s"""
         |SELECT basearea,
         |       crystalid,
         |       nowtime,
         |       state,
         |       diameter,
         |       setdiameter,
         |       mainheater,
         |       setmainheater,
         |        bottomheater,                                                                                                                               --底加功率
         |        setbottomheater, --设定底加功率
         |        lag(setbottomheater, 1) over(partition BY basearea, crystalid order by nowtime) as last_sbheater,                                                --设定底加功率lag
         |        lag(setmainheater, 1) over(partition BY basearea, crystalid order by nowtime) as last_smheater,                                                  --设定主加功率lag
         |        lag(mainheater, 1) over(partition BY basearea, crystalid order by nowtime) as last_mheater,                                                  --设定主加功率lag
         |        lag(bottomheater, 1) over(partition BY basearea, crystalid order by nowtime) as last_bheater,                                                  --设定主加功率lag
         |        lag(nowtime, 1) over(partition BY basearea, crystalid order by nowtime) as lasttime,
         |        lag(state, 1) over(partition by basearea, crystalid order by nowtime) as laststate,
         |        avgslspeed,                                                                                                                                 --平均拉速
         |        setslspeed,                                                                                                                                 --设定拉速
         |        meltsurftemp,                                                                                                                               --液温
         |        setmeltsurftemp,                                                                                                                            --设定液温
         |        meltlevel,                                                                                                                                  --液口距
         |        setmeltlevel,                                                                                                                               --设定液口距
         |        seedlift,                                                                                                                                   --晶升
         |        cruciblelift,                                                                                                                               --埚升
         |        seedrotation,                                                                                                                               --晶转
         |        cruciblerotation,                                                                                                                           --埚转
         |        argonflow,                                                                                                                                  --氩气流量
         |        mainpressure100t,                                                                                                                           --主室压力
         |        crystallength,                                                                                                                              --晶长
         |        cruciblepos,                                                                                                                                --埚位
         |        crystalpos,                                                                                                                                 --晶位
         |        crystalweight,                                                                                                                              --晶重
         |        remainweight,                                                                                                                               --剩料量
         |        pumpfrequency,                                                                                                                              --干泵频率
         |        mainheatercurrent,                                                                                                                          --主加电流
         |        bottomheatercurrent,                                                                                                                        --底加电流
         |        heatexchangerpos,                                                                                                                           --换热器位置
         |        round(runtime / 60, 1) as runduration,  --运行时长                                                                                          --加料量
         |        setpumpfrequency,                                                                                                                           --设定干泵频率
         |        feedervalveclose,                                                                                                                           --闸阀关
         |        feedervalveopen,                                                                                                                            --闸阀开
         |        isolationvalveopen,                                                                                                                         --隔离阀开
         |        isolationvalveclose,
         |        substring(nowtime, 18, 2) as nday
         | FROM ${djzkResModel}
         |  WHERE day_id >= '${daysbefore}'
         |   and day_id <= '${curday}'
         |   and mod(cast(substring (nowtime, 18, 2) as int), 2)=0
         |   and mainheater > 10
      """.stripMargin)
    //    ods_table.show(10)
    ods_table.createOrReplaceTempView("djzk_app_result")

    // RCZ分组--剔除回熔
    val ods_spark_df = spark.sql(
      s"""
         |SELECT *
         |  FROM ( SELECT *,
         |              substr(basearea, 1, 2) as base,   --基地
         |              SUBSTR(BASEAREA, -3) as puller,    --炉号
         |              case when ((last_sbheater + last_smheater < 120)
         |                  and (setbottomheater + setmainheater >= 120)) then 1 else 0 end xisopenbot, -- 是否开底加
         |              case when ((state > 4) and (state <= 9)) then state else 0 end fill_state
         |         FROM djzk_app_result
         | ) A
      """.stripMargin)
    ods_spark_df.createOrReplaceTempView("ods_table")
    //    ods_spark_df.where("xisopenbot>0").show(20)
    // 数据分组，开底加分组，调温分组、引晶分组、放肩分组、转肩分组、等径分组、收尾分组
    val group_df = spark.sql(
      s"""
         |SELECT *,
         |        CASE WHEN ((BGROUP > 0) AND (STATE = 4) AND (laststate != 4)) THEN 1 ELSE 0 END XISTE, -- 调温分组
         |        CASE WHEN ((BGROUP > 0) AND (STATE = 5) AND
         |                  (laststate != 5)) THEN 1 ELSE 0 END      XISSE,  -- 引晶分组
         |        CASE WHEN ((BGROUP > 0) AND (STATE = 6) AND
         |                  (laststate != 6)) THEN 1 ELSE 0 END      XISSH,  -- 放肩分组
         |        CASE WHEN ((BGROUP > 0) AND (STATE = 7) AND
         |                  (laststate != 7)) THEN 1 ELSE 0 END      XISTU,  -- 转肩分组
         |        CASE WHEN ((BGROUP > 0) AND (STATE = 8) AND
         |                  (laststate != 8)) THEN 1 ELSE 0 END      XISME,  -- 等径分组
         |        CASE WHEN ((BGROUP > 0) AND (STATE = 9) AND
         |                  (laststate != 9)) THEN 1 ELSE 0 END      XISEN,  -- 收尾分组
         |        CASE WHEN ((last_sbheater + last_smheater >= 120)
         |                  and (setbottomheater + setmainheater < 120)) THEN 1 ELSE 0 END  XISCB  -- 关底加分组
         | FROM (
         |          SELECT *,
         |                 SUM(ISOPENBOT) OVER(partition by basearea order by nowtime) BGROUP     ---- RCZ段数
         |          FROM (
         |                   SELECT *,
         |                          CASE WHEN (MAXSTATE IN (4, 5, 6, 7, 8, 9)) THEN XISOPENBOT ELSE 0 END ISOPENBOT
         |                   FROM (
         |                            SELECT *,
         |                                   MAX(fill_state) OVER(PARTITION BY basearea, XBGROUP) MAXSTATE
         |                            FROM (
         |                                     SELECT *,
         |                                            SUM(XISOPENBOT) OVER(PARTITION BY basearea ORDER BY NOWTIME) XBGROUP   -- RCZ
         |                                     FROM ods_table
         |                                 ) second_layer_df
         |                        ) THIRD_LAYER_DF
         |               ) FOURTH_LAYER_DF
         |      ) FIFTH_LAYER_DF
      """.stripMargin).where("BGROUP>0")
    group_df.createOrReplaceTempView("view_table01")
    println("*************** ods_table **************")

    val step_df = spark.sql(
      s"""
         |SELECT basearea,                       --片区
         |        base,                           --基地
         |        SUBSTR(PULLER, 1, 1) as AREA,   --片区
         |        puller,                         --炉号
         |        crystalid,   --晶编
         |        nday,        --天
         |        nowtime,                        --时间
         |        lasttime,                       --shift(1)位值
         |        state,                          --工步
         |        diameter,                       --直径
         |        setdiameter,                    --设定直径
         |        mainheater,                     --主加功率
         |        last_mheater,
         |        setmainheater,                 --设定主加功率
         |        bottomheater,                  --底加功率
         |        last_bheater,
         |        setbottomheater,               --设定底加功率
         |        avgslspeed,                    --平均拉速
         |        setslspeed,                    --设定拉速
         |        meltsurftemp,                  --液温
         |        setmeltsurftemp,               --设定液温
         |        meltlevel,                     --液口距
         |        setmeltlevel,                  --设定液口距
         |        seedlift,                      --晶升
         |        cruciblelift,                  --埚升
         |        seedrotation,                  --晶转
         |        cruciblerotation,              --埚转
         |        argonflow,                     --氩气流量
         |        mainpressure100t,              --主室压力
         |        crystallength,                 --晶长
         |        cruciblepos,                   --埚位
         |        crystalpos,                    --晶位
         |        crystalweight,                 --晶重
         |        remainweight,                  --剩料量
         |        pumpfrequency,                 --干泵频率
         |        mainheatercurrent,             --主加电流
         |        bottomheatercurrent,           --底加电流
         |        heatexchangerpos,              --换热器位置
         |        runduration,                   --运行时长
         |        setpumpfrequency,              --设定干泵频率
         |        feedervalveclose,              --闸阀关
         |        feedervalveopen,               --闸阀开
         |        isolationvalveopen,            --隔离阀开
         |        isolationvalveclose,
         |        TEGP,
         |        -- SUM(ISTE) OVER(PARTITION BY basearea, bgroup order by nowtime) as TEGP,    -- 调温分组
         |        SUM(ISSE) OVER(PARTITION BY basearea, bgroup order by nowtime) as SEGP,    -- 引晶分组
         |        SUM(ISSH) OVER(PARTITION BY basearea, bgroup order by nowtime) as SHGP,    -- 放肩分组
         |        SUM(ISTU) OVER(PARTITION BY basearea, bgroup order by nowtime) as TUGP,    -- 转肩分组
         |        SUM(ISME) OVER(PARTITION BY basearea, bgroup order by nowtime) as MEGP,    -- 等径分组
         |        SUM(ISEN) OVER(PARTITION BY basearea, bgroup order by nowtime) as ENGP,    -- 收尾分组
         |        SUM(ISCB) OVER(PARTITION BY basearea, bgroup order by nowtime) as CBGP,    -- 关底加分组
         |        BGROUP   -- 开底加分组
         |   FROM (
         |        SELECT *,
         |             CASE WHEN max_XSEGP IN (4, 5, 6, 7, 8, 9) THEN XISSE ELSE 0 END ISSE,
         |             CASE WHEN max_XSHGP IN (4, 5, 6, 7, 8, 9) THEN XISSH ELSE 0 END ISSH,
         |             CASE WHEN max_XTUGP IN (4, 5, 6, 7, 8, 9) THEN XISTU ELSE 0 END ISTU,
         |             CASE WHEN max_XMEGP IN (4, 5, 6, 7, 8, 9) THEN XISME ELSE 0 END ISME,
         |             CASE WHEN max_XENGP IN (4, 5, 6, 7, 8, 9) THEN XISEN ELSE 0 END ISEN,
         |             CASE WHEN max_XCBGP IN (4, 5, 6, 7, 8, 9) THEN XISCB ELSE 0 END ISCB
         |        FROM (
         |            SELECT *,
         |                  MAX(fill_state) OVER(PARTITION BY basearea, BGROUP, XSEGP) as max_XSEGP,
         |                  MAX(fill_state) OVER(PARTITION BY basearea, BGROUP, XSHGP) as max_XSHGP,
         |                  MAX(fill_state) OVER(PARTITION BY basearea, BGROUP, XTUGP) as max_XTUGP,
         |                  MAX(fill_state) OVER(PARTITION BY basearea, BGROUP, XMEGP) as max_XMEGP,
         |                  MAX(fill_state) OVER(PARTITION BY basearea, BGROUP, XENGP) as max_XENGP,
         |                  MAX(fill_state) OVER(PARTITION BY basearea, BGROUP, XCBGP) as max_XCBGP
         |            FROM (
         |                  SELECT *,
         |                        SUM(XISTE) OVER(PARTITION BY basearea, bgroup ORDER BY nowtime) TEGP,   -- 调温分组
         |                        SUM(XISSE) OVER(PARTITION BY basearea, bgroup ORDER BY nowtime) XSEGP,
         |                        SUM(XISSH) OVER(PARTITION BY basearea, bgroup ORDER BY nowtime) XSHGP,
         |                        SUM(XISTU) OVER(PARTITION BY basearea, bgroup ORDER BY nowtime) XTUGP,
         |                        SUM(XISME) OVER(PARTITION BY basearea, bgroup ORDER BY nowtime) XMEGP,
         |                        SUM(XISEN) OVER(PARTITION BY basearea, bgroup ORDER BY nowtime) XENGP,
         |                        SUM(XISCB) OVER(PARTITION BY basearea, bgroup ORDER BY nowtime) XCBGP   -- 关底加分组
         |                   FROM view_table01
         |                 ) A
         |            ) B
         |       ) C
        """.stripMargin)
    step_df.createOrReplaceTempView("first_layer_table")
    // ----------------------------------
    val temp_df01 = spark.sql(
      s"""
         |select *,
         |    case when stepno8 >0 then min(nowtime) over(PARTITION BY basearea, bgroup, stepno8)
         |        else null end as xeig_starttime,     -- 等径起始时刻
         |    case when stepno8 >0 then
         |         round((unix_timestamp(max(nowtime) over(PARTITION BY basearea, bgroup, stepno8))
         |               - unix_timestamp(min(nowtime) over(PARTITION BY basearea, bgroup, stepno8))
         |               )/60, 1) else null end as xeig_duration,         -- 等径持续时长
         |    case when stepno8 >0 then first_value(cruciblepos) over (PARTITION BY basearea, bgroup, stepno8 order by nowtime)
         |         else null end as xeig_initial_cposition,   -- 等径初始埚位
         |    case when stepno8 >0 then last_value(cruciblepos) over (PARTITION BY basearea, bgroup, stepno8 order by nowtime
         |         rows between unbounded preceding and unbounded following)
         |         else null end as xeig_end_cposition,       -- 等径终止埚位
         |    case when stepno8 >0 then min(mainheater) over (PARTITION BY basearea, bgroup, stepno8)
         |         else null end as xeig_min_mheater,         -- 等径全程最小主加
         |    case when stepno8 >0 then max(mainheater) over (PARTITION BY basearea, bgroup, stepno8)
         |         else null end as xeig_max_mheater,         -- 等径全程最大主加
         |    case when stepno8 >0 then first_value(setdiameter) over (PARTITION BY basearea, bgroup, stepno8 order by nowtime)
         |         else null end as xeig_initial_sdiameter,         -- 等径初始设定直径
         |    case when stepno8 >0 then round(avg(bottomheater) over (PARTITION BY basearea, bgroup, stepno8), 1)
         |         else null end as xeig_avg_bheater,         -- 等径主加均值
         |    case when stepno8 >0 then min(meltlevel) over (PARTITION BY basearea, bgroup, stepno8)
         |         else null end as xeig_min_meltlevel,       -- 等径最小液口距
         |    case when stepno8 >0 then last_value(meltlevel) over (PARTITION BY basearea, bgroup, stepno8 order by nowtime desc)
         |         else null end as xeig_end_meltlevel,       -- 等径终止液口距
         |    case when stepno8 >0 then
         |        first_value(crystallength) over (PARTITION BY basearea, bgroup,  stepno8 order by nowtime desc) else null end as xeig_end_crystallength,   -- 等径结束长度
         |    case when stepno8 >0 then
         |         round(avg(mainpressure100t) over (PARTITION BY basearea, bgroup, stepno8), 1)
         |         else null end as xeig_avg_mpressure,       -- 等径炉压均值
         |    case when stepno8 >0 then
         |         round(avg(argonflow) over (PARTITION BY basearea, bgroup, stepno8), 1)
         |         else null end as xeig_avg_argonflow,       -- 等径氩气流量均值
         |    case when stepno8 >0 then
         |         round(avg(pumpfrequency) over (PARTITION BY basearea, bgroup, stepno8), 1)
         |         else null end as xeig_avg_pumpfrequency,       -- 等径干泵频率均值
         |    case when stepno8 >0 then
         |         min(remainweight) over (PARTITION BY basearea, bgroup, stepno8)
         |         else null end as xeig_end_reweight,         -- 等径终止炉内剩料
         |    case when (stepno8 >0) and (crystallength <= 300) then
         |         cruciblerotation else null end as xeig_front300_crotation,     -- 等径前300mm埚转
         |    case when (stepno8 >0) and (unix_timestamp(
         |         max(nowtime) over(PARTITION BY basearea, bgroup, stepno8))-unix_timestamp(nowtime) <= 1800)
         |         then cruciblerotation else null end as xeig_rear_30crotation,   -- 等径后30min埚转
         |    case when (stepno8 >0) and (crystallength <= 500) then
         |         cruciblelift else null end as xeig_front500_clift,              -- 等径前500mm埚升
         |    case when (stepno8 >0) and (unix_timestamp(nowtime) - unix_timestamp(
         |         min(nowtime) over(PARTITION BY basearea, bgroup, stepno8)) <= 1800)
         |         then seedrotation else null end as xeig_front_30srotation,        -- 等径前30min晶转
         |
         |    case when (stepno8 >0) and (unix_timestamp(
         |         max(nowtime) over(PARTITION BY basearea, bgroup, stepno8))
         |         - unix_timestamp(nowtime) <= 1800)
         |         then seedrotation else null end as xeig_rear_30srotation,        -- 等径后30min晶转
         |    case when (stepno8 >0) and (unix_timestamp(
         |         max(nowtime) over(PARTITION BY basearea, bgroup, stepno8))
         |         - unix_timestamp(nowtime) <= 600)
         |         then crystallength else null end as xeig_rear_10clength,         -- 等径后10min晶棒长度
         |
         |    case when (stepno8 >0) and (crystallength <= 100) then
         |         cruciblelift else null end as xeig_local100_clift,            -- 等径0-100mm埚升
         |    case when (stepno8 >0) and (crystallength <= 200) and (crystallength > 100)
         |         then cruciblelift else null end as xeig_local200_clift,       -- 等径100-200mm埚升
         |    case when (stepno8 >0) and (crystallength <= 300) and (crystallength > 200)
         |         then cruciblelift else null end as xeig_local300_clift,       -- 等径200-300mm埚升
         |    case when (stepno8 >0) and (crystallength <= 400) and (crystallength > 300)
         |         then cruciblelift else null end as xeig_local400_clift,       -- 等径300-400mm埚升
         |    case when (stepno8 >0) and (crystallength <= 500) and (crystallength > 400)
         |         then cruciblelift else null end as xeig_local500_clift,       -- 等径400-500mm埚升
         |
         |    --- 2023-04-24 增加等径工步主加功率 （瞬时主加功率分组）
         |    CASE WHEN (stepno8 > 0) and (crystallength <= 100) THEN 1
         |         WHEN (stepno8 > 0) and (crystallength >100) and (crystallength <= 200) THEN 2
         |         WHEN (stepno8 > 0) and (crystallength >200) and (crystallength <= 300) THEN 3
         |         WHEN (stepno8 > 0) and (crystallength >300) and (crystallength <= 400) THEN 4
         |         WHEN (stepno8 > 0) and (crystallength >400) and (crystallength <= 500) THEN 5
         |         WHEN (stepno8 > 0) and (crystallength >500) and (crystallength <= 1000) THEN 6
         |         WHEN (stepno8 > 0) and (crystallength >1000) and (crystallength <= 1500) THEN 7
         |         WHEN (stepno8 > 0) and (crystallength >1500) and (crystallength <= 2000) THEN 8
         |         WHEN (stepno8 > 0) and (crystallength >2000) and (crystallength <= 2500) THEN 9
         |         WHEN (stepno8 > 0) and (crystallength >2500) and (crystallength <= 3000) THEN 10
         |         WHEN (stepno8 > 0) and (crystallength >3000) and (crystallength <= 3500) THEN 11
         |         ELSE 12 END AS dj_group,                                        -- 等径分组
         |
         |    case when (stepno8 >0) and (crystallength <= 500) then
         |         crystallength else null end as xeig_front500_clength,            -- 等径前500mm晶长
         |    case when (stepno8 >0) and (crystallength > 500) and (crystallength <= 1200) then
         |         crystallength else null end as xeig_mid1200_clength,             -- 等径500-1200mm晶长
         |    case when (stepno8 >0) and (crystallength <= 500) then
         |         nowtime else null end as xeig_front500_nt,                       -- 等径前500mm时间
         |    case when (stepno8 >0) and (crystallength > 500) and (crystallength <= 1200) then
         |         nowtime else null end as xeig_mid1200_nt,                        -- 等径500-1200mm时间
         |    case when (stepno8 >0) and (crystallength <= 150) then
         |         diameter else null end as xeig_front150_diameter,                -- 等径前150mm直径
         |    case when (stepno8 >0) and (crystallength <= 150) then
         |         setdiameter else null end as xfront150_setdiameter,              -- 等径前150mm设定直径
         |    case when (stepno8 >0) and (crystallength <= 5) then
         |         diameter else null end as xeig_front5_diameter,                  -- 等径前5mm直径
         |    -- ! 新增2022-07-22 （开始）！--
         |    case when stepno8 >0 then round(avg(setmeltlevel) over(PARTITION BY basearea, bgroup, stepno8), 1)
         |         else null end as xeig_avgsmeltlevel,                             -- 设定目标液口距均值
         |    case when (stepno8 >0) and (crystallength<=500) then
         |         meltlevel else null end as xeig_local500_meltlevel,       -- 0-500液口距均值
         |    case when (stepno8 >0) and (crystallength > 500) and (crystallength <= 1000) then
         |         meltlevel else null end as xeig_local1000_meltlevel,      -- 500-1000液口距均值
         |    case when (stepno8 >0) and (crystallength > 1000) and (crystallength <= 1500) then
         |         meltlevel else null end as xeig_local1500_meltlevel,      -- 1000-1500液口距均值
         |    case when (stepno8 >0) and abs(meltlevel-setmeltlevel) <= 1 then
         |         crystallength else null end as xeig_lt1_clength,     -- 液口距偏差<1等径长度
         |    --！PLC拉速均值 ！--
         |    case when (stepno8 >0) and (crystallength > 0 and crystallength <= 100) then
         |         avgslspeed else null end as xeig_local100_speed,          -- 等径0-100mmPLC拉速
         |    case when (stepno8 >0) and (crystallength > 100 and crystallength <= 200) then
         |         avgslspeed else null end as xeig_local200_speed,          -- 等径100-200mmPLC拉速
         |    case when (stepno8 >0) and (crystallength > 200 and crystallength <= 300) then
         |         avgslspeed else null end as xeig_local300_speed,          -- 等径200-300mmPLC拉速
         |    case when (stepno8 >0) and (crystallength > 300 and crystallength <= 400) then
         |         avgslspeed else null end as xeig_local400_speed,          -- 等径300-400mmPLC拉速
         |    case when (stepno8 >0) and (crystallength > 400 and crystallength <= 500) then
         |         avgslspeed else null end as xeig_local500_speed,          -- 等径400-500mmPLC拉速
         |    case when (stepno8 >0) and (crystallength > 500 and crystallength <= 1200) then
         |         avgslspeed else null end as xeig_local1200_speed,          -- 等径500-1200mmPLC拉速
         |    case when (stepno8 >0) and (crystallength > 1000 and crystallength <= 1500) then
         |         setslspeed else null end as xeig_local1500_setspeed,       -- 等径1000-1500mmPLC拉速
         |    case when stepno8 >0 then avg(avgslspeed) over(PARTITION BY basearea, bgroup, stepno8)
         |         else null end as xeig_entirety_avgspeed,                  -- 等径全程拉速均值
         |
         |    -- 拉速偏差标准差 100,200,300,400,500,1200
         |    case when (stepno8 >0) and (crystallength > 0 and crystallength <= 100) then
         |         setslspeed - avgslspeed else null end as xeig_local100_diffspeed,          -- 等径0-100mmPLC拉速偏差
         |    case when (stepno8 >0) and (crystallength > 100 and crystallength <= 200) then
         |         setslspeed - avgslspeed else null end as xeig_local200_diffspeed,          -- 等径100-200mmPLC拉速偏差
         |    case when (stepno8 >0) and (crystallength > 200 and crystallength <= 300) then
         |         setslspeed - avgslspeed else null end as xeig_local300_diffspeed,          -- 等径200-300mmPLC拉速偏差
         |    case when (stepno8 >0) and (crystallength > 300 and crystallength <= 400) then
         |         setslspeed - avgslspeed else null end as xeig_local400_diffspeed,          -- 等径300-400mmPLC拉速偏差
         |    case when (stepno8 >0) and (crystallength > 400 and crystallength <= 500) then
         |         setslspeed - avgslspeed else null end as xeig_local500_diffspeed,          -- 等径400-500mmPLC拉速偏差
         |    case when (stepno8 >0) and (crystallength > 500 and crystallength <= 1200) then
         |         setslspeed - avgslspeed else null end as xeig_local1200_diffspeed,         -- 等径500-1200mmPLC拉速偏差
         |
         |    -- 直径值标准差 100,200,300,400,500,1200
         |    case when (stepno8 >0) and (crystallength > 0 and crystallength <= 100) then
         |         diameter else null end as xeig_local100_diameter,          -- 等径0-100mm直径
         |    case when (stepno8 >0) and (crystallength > 100 and crystallength <= 200) then
         |         diameter else null end as xeig_local200_diameter,          -- 等径100-200mm直径
         |    case when (stepno8 >0) and (crystallength > 200 and crystallength <= 300) then
         |         diameter else null end as xeig_local300_diameter,          -- 等径200-300mm直径
         |    case when (stepno8 >0) and (crystallength > 300 and crystallength <= 400) then
         |         diameter else null end as xeig_local400_diameter,          -- 等径300-400mm直径
         |    case when (stepno8 >0) and (crystallength > 400 and crystallength <= 500) then
         |         diameter else null end as xeig_local500_diameter,          -- 等径400-500mm直径
         |    case when (stepno8 >0) and (crystallength > 500 and crystallength <= 1200) then
         |         diameter else null end as xeig_local1200_diameter,         -- 等径500-1200mm直径
         |
         |    -- 直径偏差标准差 100,200,300,400,500,1200
         |    case when (stepno8 >0) and (crystallength > 0 and crystallength <= 100) then
         |         setdiameter - diameter else null end as xeig_local100_diffdiameter,          -- 等径0-100mm直径偏差
         |    case when (stepno8 >0) and (crystallength > 100 and crystallength <= 200) then
         |         setdiameter - diameter else null end as xeig_local200_diffdiameter,          -- 等径100-200mm直径偏差
         |    case when (stepno8 >0) and (crystallength > 200 and crystallength <= 300) then
         |         setdiameter - diameter else null end as xeig_local300_diffdiameter,          -- 等径200-300mm直径偏差
         |    case when (stepno8 >0) and (crystallength > 300 and crystallength <= 400) then
         |         setdiameter - diameter else null end as xeig_local400_diffdiameter,          -- 等径300-400mm直径偏差
         |    case when (stepno8 >0) and (crystallength > 400 and crystallength <= 500) then
         |         setdiameter - diameter else null end as xeig_local500_diffdiameter,          -- 等径400-500mm直径偏差
         |    case when (stepno8 >0) and (crystallength > 500 and crystallength <= 1200) then
         |         setdiameter - diameter else null end as xeig_local1200_diffdiameter,         -- 等径500-1200mm直径偏差
         |    -- ! 新增2022-07-22 （结束）！--
         |
         |    -- !收尾指标提取! --
         |    case when stepno9 >0 then
         |         min(nowtime) over(PARTITION BY basearea, bgroup, stepno9)
         |         else null end as xnin_starttime,     -- 收尾起始时刻
         |    case when stepno9 >0 then
         |         round((unix_timestamp(max(nowtime) over(PARTITION BY basearea, bgroup, stepno9))
         |         - unix_timestamp(min(nowtime) over(PARTITION BY basearea, bgroup, stepno9)))/60, 1)
         |         else null end as xnin_duration,         -- 收尾持续时长
         |    case when stepno9 >0 then
         |         last_value(remainweight) over (PARTITION BY basearea, bgroup, stepno9 order by nowtime
         |         rows between unbounded preceding and unbounded following)
         |         else null end as xnin_end_reweight,         -- 收尾终止炉内剩料
         |    case when stepno9 >0 then
         |         round(max(crystalweight) over(PARTITION BY basearea, bgroup, stepno9)
         |         - min(crystalweight) over(PARTITION BY basearea, bgroup, stepno9), 1)
         |         else null end as xnin_tail_weight,          -- 收尾尾巴重量
         |    case when stepno9 >0 then
         |         round(max(crystallength) over(PARTITION BY basearea, bgroup, stepno9)
         |         - min(crystallength) over(PARTITION BY basearea, bgroup, stepno9), 1)
         |         else null end as xnin_tail_length,          -- 收尾尾巴重量
         |    row_number() over(partition by basearea, bgroup, stepno9 order by nowtime) rank9,
         |
         |    -- !化料指标提取! --
         |     max(xishead) over(PARTITION BY basearea, bgroup) as fishead,
         |    case when stepno2 >0 then
         |                    min(nowtime) over(PARTITION BY basearea, bgroup, stepno2)
         |                    else null end as xtwo_starttime,        -- 化料起始时刻
         |    case when stepno2 >0 then
         |                    round((unix_timestamp(max(nowtime) over(PARTITION BY basearea, bgroup, stepno2))
         |                    - unix_timestamp(min(nowtime) over(PARTITION BY basearea, bgroup, stepno2)))/60, 1)
         |                    else null end as xtwo_duration,         -- 化料持续时长
         |    case when stepno2 >0 then
         |                    ((last_mheater + mainheater) * difftime)/7200
         |                    else null end as xtwo_mheater_area,         -- 化料主加面积积分
         |    case when stepno2 >0 then
         |                    ((last_bheater + bottomheater) * difftime)/7200
         |                    else null end as xtwo_bheater_area,         -- 化料底加面积积分
         |    case when stepno2 >0 then
         |                    min(remainweight) over(PARTITION BY basearea, bgroup, stepno2)
         |                    else null end as xtwo_initial_reweight,   -- 化料初始投料量
         |    case when stepno2 >0 then
         |                    max(remainweight) over(PARTITION BY basearea, bgroup, stepno2)
         |                    else null end as xtwo_end_reweight,       -- 化料终止投料量
         |    case when stepno2 >0 then
         |                    mainheater/(mainheatercurrent * mainheatercurrent)
         |                    else null end as xquotient_mresistance,    -- 化料主加电阻
         |    case when stepno2 >0 then
         |                    bottomheater/(bottomheatercurrent * bottomheatercurrent)
         |                    else null end as xquotient_bresistance,    -- 化料底加电阻
         |    case when stepno2 >0 then
         |                    avg(mainpressure100t) over(PARTITION BY basearea, bgroup, stepno2)
         |                    else null end as xtwo_avg_mpressure,       -- 化料炉压均值
         |    case when stepno2 >0 then
         |                    min(mainpressure100t) over(PARTITION BY basearea, bgroup, stepno2)
         |                    else null end as xtwo_min_mpressure,       -- 化料炉压最小值
         |    case when stepno2 >0 then
         |                    max(mainpressure100t) over(PARTITION BY basearea, bgroup, stepno2)
         |                    else null end as xtwo_max_mpressure,       -- 化料炉压最大值
         |    case when stepno2 >0 then
         |                    round(stddev(mainpressure100t) over(PARTITION BY basearea, bgroup, stepno2), 1)
         |                    else null end as xtwo_std_mpressure,       -- 化料炉压标准差
         |    case when stepno2 >0 then
         |                    round(avg(mainheater) over(PARTITION BY basearea, bgroup, stepno2), 1)
         |                    else null end as xtwo_avg_mheater,         -- 化料主加功率均值
         |    case when stepno2 >0 then
         |                    round(avg(bottomheater) over(PARTITION BY basearea, bgroup, stepno2), 1)
         |                    else null end as xtwo_avg_bheater,         -- 化料底加功率均值
         |    case when stepno2 >0 then
         |                    last_value(cruciblepos) over(PARTITION BY basearea, bgroup, stepno2 order by nowtime
         |                    rows between unbounded preceding and unbounded following)
         |                    else null end as xtwo_end_cposition,       -- 化料终止埚位
         |    case when (stepno2 >0) and (unix_timestamp(
         |          max(nowtime) over(PARTITION BY basearea, bgroup, stepno2)
         |          ) - unix_timestamp(nowtime) <= 300)  then
         |                    cruciblerotation else null end as xtwo_last_5crotation,    -- 化料后5min埚转
         |
         |    -- add comment:隔离阀由开-关闭或者闸阀由开-关闭=>定义为1,否则为空
         |    case when stepno2 >0 and (feedervalveopen = 1 or isolationvalveopen = 1) then nowtime else null end as open_nt,
         |
         |    -- 化料阶段新增2022-07-22 --
         |    case when stepno2 >0 then max(setmainheater) over(PARTITION BY basearea, bgroup, stepno2)
         |         else null end as xtwo_max_setmheater,
         |    case when stepno2 >0 then max(setbottomheater) over(PARTITION BY basearea, bgroup, stepno2)
         |         else null end as xtwo_max_setbheater,
         |    case when stepno2 >0 then round(avg(argonflow) over(PARTITION BY basearea, bgroup, stepno2), 1)
         |         else null end as xtwo_avg_argonflow,
         |
         |    -- !准备调温指标提取! --
         |    case when stepno3 >0 then
         |         min(nowtime) over(PARTITION BY basearea, bgroup, stepno3)
         |         else null end as xthr_starttime,        -- 准备调温起始时刻
         |    case when stepno3 >0 then
         |                    round((unix_timestamp(max(nowtime) over(PARTITION BY basearea, bgroup, stepno3))
         |                    - unix_timestamp(min(nowtime) over(PARTITION BY basearea, bgroup, stepno3)))/60, 1)
         |                    else null end as xthr_duration,         -- 准备调温持续时长
         |    case when stepno3 >0 then
         |                    ((last_mheater + mainheater) * difftime)/7200
         |                    else null end as xthr_mheater_area,         -- 准备调温主加面积积分
         |    case when stepno3 >0 then
         |                    ((last_bheater + bottomheater) * difftime)/7200
         |                    else null end as xthr_bheater_area,         -- 准备调温底加面积积分
         |    case when stepno3 >0 then
         |                    max(remainweight) over(PARTITION BY basearea, bgroup, stepno3)
         |                    else null end as xthr_end_reweight,     -- 准备调温终止投料量
         |    case when stepno3 >0 then
         |                    last_value(cruciblepos) over(PARTITION BY basearea, bgroup, stepno3 order by nowtime
         |                    rows between unbounded preceding and unbounded following)
         |                    else null end as xthr_end_cposition,     -- 准备调温终止埚位
         |    case when (stepno3 >0) and (unix_timestamp(max(nowtime)
         |                        over(PARTITION BY basearea, bgroup, stepno3)) - unix_timestamp(nowtime) <= 300)  then
         |                    cruciblerotation else null end as xthr_last_5crotation,    -- 准备调温后5min埚转
         |    case when (stepno3 >0) and (unix_timestamp(max(nowtime)
         |                        over(PARTITION BY basearea, bgroup, stepno3)) - unix_timestamp(nowtime) <= 300)  then
         |                    mainpressure100t else null end as xthr_last_5mpressure,    -- 准备调温后5min炉压
         |    case when (stepno3 >0) and (unix_timestamp(max(nowtime)
         |                        over(PARTITION BY basearea, bgroup, stepno3)) - unix_timestamp(nowtime) <= 300)  then
         |                    meltlevel else null end as xthr_last_5meltlevel,    -- 准备调温后5min液口距
         |    case when stepno3 >0 then
         |                    (meltsurftemp * difftime)/3600
         |                    else null end as xthr_surftemp_area,         -- 准备调温实际液温面积积分
         |    case when stepno3 >0 then
         |                    max(diffsurftemp) over(PARTITION BY basearea, bgroup, stepno3)
         |                    else null end as xthr_max_diffsurftemp,     -- 准备调温最大液温差
         |    case when stepno3 >0 then
         |                    min(diffsurftemp) over(PARTITION BY basearea, bgroup, stepno3)
         |                    else null end as xthr_min_diffsurftemp,     -- 准备调温最小液温差
         |    case when stepno3 >0 then
         |                    stddev(diffsurftemp) over(PARTITION BY basearea, bgroup, stepno3)
         |                    else null end as xthr_std_diffsurftemp,     -- 准备调温液温差标准差
         |
         |    -- *********2022-11-08 add 埚转>5/功率降 标记
         |    case when stepno3 >0 and cruciblerotation > 5 then 1 else 0 end as xthr_mark,
         |
         |    -- !调温指标提取! --
         |    case when stepno4 >0 then
         |                    min(nowtime) over(PARTITION BY basearea, bgroup, stepno4)
         |                    else null end as xfou_starttime,        -- 调温起始时刻
         |    case when stepno4 >0 then
         |                    round((unix_timestamp(max(nowtime) over(PARTITION BY basearea, bgroup, stepno4))
         |                    - unix_timestamp(min(nowtime) over(PARTITION BY basearea, bgroup, stepno4)))/60, 1)
         |                    else null end as xfou_duration,         -- 调温持续时长
         |    case when stepno4 >0 then
         |                    (meltsurftemp * difftime)/3600
         |                    else null end as xfou_surftemp_area,         -- 调温实际液温面积积分
         |    case when stepno4 >0 then
         |                    min(setmainheater) over(PARTITION BY basearea, bgroup, stepno4)
         |                    else null end as xfou_min_smheater,          -- 调温最低主加设定功率
         |    case when stepno4 >0 then
         |                    first_value(mainheater) over(PARTITION BY basearea, bgroup, stepno4 order by nowtime)
         |                    else null end as xfou_initial_mheater,            -- 调温初始主加值
         |    case when stepno4 >0 then
         |                    round(avg(bottomheater) over(PARTITION BY basearea, bgroup, stepno4), 1)
         |                    else null end as xfou_avg_bheater,            -- 调温底加功率均值
         |    case when stepno4 >0 then
         |                    round(avg(meltlevel) over(PARTITION BY basearea, bgroup, stepno4), 1)
         |                    else null end as xfou_avg_meltlevel,            -- 调温液口距均值
         |    case when stepno4 >0 then
         |                    round(avg(cruciblerotation) over(PARTITION BY basearea, bgroup, stepno4), 1)
         |                    else null end as xfou_avg_crotation,            -- 调温埚转均值
         |    case when stepno4 >0 then
         |                    round(avg(cruciblepos) over(PARTITION BY basearea, bgroup, stepno4), 1)
         |                    else null end as xfou_avg_cposition,            -- 调温埚位均值
         |    case when stepno4 >0 then
         |                    round(avg(mainpressure100t) over(PARTITION BY basearea, bgroup, stepno4), 1)
         |                    else null end as xfou_avg_mpressure,            -- 调温炉压均值
         |    case when stepno4 >0 then
         |                    round(avg(argonflow) over(PARTITION BY basearea, bgroup, stepno4), 1)
         |                    else null end as xfou_avg_argonflow,            -- 调温氩气流量均值
         |    case when stepno4 >0 then
         |                    min(crystalpos) over(PARTITION BY basearea, bgroup, stepno4)
         |                    else null end as xfou_min_crystalpos,            -- 调温晶体最小位置
         |    case when stepno4 >0 then
         |                    first_value(heatexchangerpos) over(PARTITION BY basearea, bgroup, stepno4 order by nowtime)
         |                    else null end as xfou_start_heatexchangerpos,    -- 调温初始换热器位置
         |    case when stepno4 >0 then
         |                    last_value(setmainheater) over(PARTITION BY basearea, bgroup, stepno4 order by nowtime rows
         |                    between unbounded preceding and unbounded following)
         |                    else null end as xfou_last_setmheater,               -- 调温结束主加功率
         |    case when stepno4 >0 then
         |                    round(avg(diffsurftemp) over(PARTITION BY basearea, bgroup, stepno4), 1)
         |                    else null end as xfou_avg_diffsurftemp,            -- 调温亮度差均值
         |    case when stepno4 >0 then
         |                    min(diffsurftemp) over(PARTITION BY basearea, bgroup, stepno4)
         |                    else null end as xfou_min_diffsurftemp,               -- 调温液面亮度差极小值
         |    case when stepno4 >0 then
         |                    max(diffsurftemp) over(PARTITION BY basearea, bgroup, stepno4)
         |                    else null end as xfou_max_diffsurftemp,               -- 调温液面亮度差极大值
         |    case when stepno4 >0 then
         |                    stddev(diffsurftemp) over(PARTITION BY basearea, bgroup, stepno4)
         |                    else null end as xfou_std_diffsurftemp,               -- 调温液面亮度差标准差
         |    case when stepno4 >0 then
         |                    first_value(diffsurftemp) over(PARTITION BY basearea, bgroup, stepno4 order by nowtime)
         |                    else null end as xfou_initial_diffsurftemp,           -- 调温初始液温差
         |
         |    -- !引晶指标提取! --
         |    case when stepno5 >0 then
         |                    min(nowtime) over(PARTITION BY basearea, bgroup, stepno5)
         |                    else null end as xfiv_starttime,        -- 引晶起始时刻
         |    case when stepno5 >0 then
         |                    round((unix_timestamp(max(nowtime) over(PARTITION BY basearea, bgroup, stepno5))
         |                    - unix_timestamp(min(nowtime) over(PARTITION BY basearea, bgroup, stepno5)))/60, 1)
         |                    else null end as xfiv_duration,         -- 引晶持续时长
         |    case when stepno5 >0 then
         |                    round(avg(mainheater) over(PARTITION BY basearea, bgroup, stepno5), 1)
         |                    else null end as xfiv_avg_mheater,            -- 引晶主加功率均值
         |    case when stepno5 >0 then
         |                    round(avg(bottomheater) over(PARTITION BY basearea, bgroup, stepno5), 1)
         |                    else null end as xfiv_avg_bheater,            -- 引晶底加功率均值
         |    case when stepno5 >0 then
         |                    round(avg(meltlevel) over(PARTITION BY basearea, bgroup, stepno5), 1)
         |                    else null end as xfiv_avg_meltlevel,            -- 引晶液口距均值
         |    case when stepno5 >0 then
         |                    round(avg(cruciblerotation) over(PARTITION BY basearea, bgroup, stepno5), 1)
         |                    else null end as xfiv_avg_crotation,            -- 引晶埚转均值
         |    case when stepno5 >0 then                    round(avg(cruciblepos) over(PARTITION BY basearea, bgroup, stepno5), 1)
         |                    else null end as xfiv_avg_cposition,            -- 引晶埚位均值
         |    case when stepno5 >0 then
         |                    round(avg(mainpressure100t) over(PARTITION BY basearea, bgroup, stepno5), 1)
         |                    else null end as xfiv_avg_mpressure,            -- 引晶炉压均值
         |    case when stepno5 >0 then
         |                    round(avg(argonflow) over(PARTITION BY basearea, bgroup, stepno5), 1)
         |                    else null end as xfiv_avg_argonflow,            -- 引晶氩气流量均值
         |    case when stepno5 >0 then
         |                    round(avg(pumpfrequency) over(PARTITION BY basearea, bgroup, stepno5), 1)
         |                    else null end as xfiv_avg_pumpfrequency,            -- 引晶干泵频率均值
         |    case when stepno5 >0 then
         |                    round(avg(seedrotation) over(PARTITION BY basearea, bgroup, stepno5), 1)
         |                    else null end as xfiv_avg_srotation,            -- 引晶晶转均值
         |    case when stepno5 >0 then
         |                    last_value(crystallength) over(PARTITION BY basearea, bgroup, stepno5 order by nowtime
         |                    rows between unbounded preceding and unbounded following)
         |                    else null end as xfiv_end_crystallength,        -- 引晶最大长度
         |    case when (stepno5 >0) and (crystallength > 100) then
         |                    diameter else null end as xfiv_rear100_diameter,  -- 引晶100mm后直径
         |    case when (stepno5 >0) and (crystallength > 100) then
         |                    avgslspeed else null end as xfiv_rear100_avgslspeed,  -- 引晶100mm后PLC拉速
         |    case when (stepno5 >0) and (crystallength > 130) then
         |                    avgslspeed else null end as xfiv_rear130_avgslspeed,  -- 引晶130mm后PLC拉速
         |    case when (stepno5 >0) and (crystallength > 160) then
         |                    avgslspeed else null end as xfiv_rear160_avgslspeed,  -- 引晶160mm后PLC拉速
         |    case when stepno5 >0 then
         |                    (diffsurftemp * difftime) / 3600
         |                    else null end as xfiv_diffsurftemp_area,        -- 引晶液温差（real-set）面积
         |    case when stepno5 >0 then
         |                    round((max(meltsurftemp) over(PARTITION BY basearea, bgroup, stepno5)
         |                        - min(meltsurftemp) over(PARTITION BY basearea, bgroup, stepno5)), 1)
         |                    else null end as xfiv_extre_diffsurftemp,       -- 引晶液温(max_real-min_real)差
         |    case when stepno5 >0 then
         |                    round(avg(diffsurftemp) over(PARTITION BY basearea, bgroup, stepno5), 1)
         |                    else null end as xfiv_avg_diffsurftemp,            -- 引晶液温差均值
         |    case when stepno5 >0 then
         |                    round(stddev(diffsurftemp) over(PARTITION BY basearea, bgroup, stepno5), 1)
         |                    else null end as xfiv_std_diffsurftemp,            -- 引晶液温差标准差
         |
         |    -- ！引晶直径<=6mm 刻的直径长度 2022-07-22(新增)
         |    case when (stepno5 >0) and (diameter <=6) then crystallength else null end as xfiv_lt6_clength,
         |    -- ！2022-07-15 增加！--
         |    case when stepno5 >0 then
         |        first_value(diameter) over(PARTITION BY basearea, bgroup, stepno5 order by nowtime)
         |        else null end as xfiv_diameter0,                -- 引晶初始直径
         |    case when (stepno5 >0) and (crystallength <= 20) then
         |         1 else 0 end as fiv_mid20_group,  -- 引晶20mm分组
         |    case when (stepno5 >0) and (crystallength <= 40) and (crystallength > 20) then
         |         1 else 0 end as fiv_mid40_group,  -- 引晶40mm分组
         |    case when (stepno5 >0) and (crystallength <= 60) and (crystallength > 40) then
         |         1 else 0 end as fiv_mid60_group,  -- 引晶60mm分组
         |    case when (stepno5 >0) and (crystallength <= 80) and (crystallength > 60) then
         |         1 else 0 end as fiv_mid80_group,  -- 引晶80mm分组
         |    case when (stepno5 >0) and (crystallength <= 100) and (crystallength > 80) then
         |         1 else 0 end as fiv_mid100_group,  -- 引晶100mm分组
         |    case when (stepno5 >0) and (crystallength <= 120) and (crystallength > 100) then
         |         1 else 0 end as fiv_mid120_group,  -- 引晶120mm分组
         |    case when (stepno5 >0) and (crystallength <= 140) and (crystallength > 120) then
         |         1 else 0 end as fiv_mid140_group,  -- 引晶140mm分组
         |    case when (stepno5 >0) and (crystallength <= 160) and (crystallength > 140) then
         |         1 else 0 end as fiv_mid160_group,  -- 引晶160mm分组
         |    case when (stepno5 >0) and (crystallength <= 180) and (crystallength > 160) then
         |         1 else 0 end as fiv_mid180_group,  -- 引晶180mm分组
         |    case when (stepno5 >0) and (crystallength <= 200) and (crystallength > 180) then
         |         1 else 0 end as fiv_mid200_group,  -- 引晶200mm分组
         |    case when stepno5 >0 then
         |        last_value(diameter) over(PARTITION BY basearea, bgroup, stepno5 order by nowtime rows
         |        between unbounded preceding and unbounded following)
         |        else null end as xfiv_diameterB,
         |
         |    -- !放肩指标提取! --
         |    case when stepno6 >0 then
         |                    min(nowtime) over(PARTITION BY basearea, bgroup, stepno6)
         |                    else null end as xsix_starttime,        -- 放肩起始时刻
         |    case when stepno6 >0 then
         |                    round((unix_timestamp(max(nowtime) over(PARTITION BY basearea, bgroup, stepno6))
         |                    - unix_timestamp(min(nowtime) over(PARTITION BY basearea, bgroup, stepno6)))/60, 1)
         |                    else null end as xsix_duration,         -- 放肩持续时长
         |    case when stepno6 >0 then
         |                    round(avg(mainheater) over(PARTITION BY basearea, bgroup, stepno6), 1)
         |                    else null end as xsix_avg_mheater,            -- 放肩主加功率均值
         |    case when stepno6 >0 then
         |                    round(avg(bottomheater) over(PARTITION BY basearea, bgroup, stepno6), 1)
         |                    else null end as xsix_avg_bheater,            -- 放肩底加功率均值
         |    case when stepno6 >0 then
         |                    round(avg(meltlevel) over(PARTITION BY basearea, bgroup, stepno6), 1)
         |                    else null end as xsix_avg_meltlevel,            -- 放肩液口距均值
         |    case when stepno6 >0 then
         |                    round(avg(cruciblerotation) over(PARTITION BY basearea, bgroup, stepno6), 1)
         |                    else null end as xsix_avg_crotation,            -- 放肩埚转均值
         |    case when stepno6 >0 then
         |                    round(avg(cruciblepos) over(PARTITION BY basearea, bgroup, stepno6), 1)
         |                    else null end as xsix_avg_cposition,            -- 放肩埚位均值
         |    case when stepno6 >0 then
         |                    round(avg(mainpressure100t) over(PARTITION BY basearea, bgroup, stepno6), 1)
         |                    else null end as xsix_avg_mpressure,            -- 放肩炉压均值
         |    case when stepno6 >0 then
         |                    round(avg(argonflow) over(PARTITION BY basearea, bgroup, stepno6), 1)
         |                    else null end as xsix_avg_argonflow,            -- 放肩氩气流量均值
         |    case when stepno6 >0 then
         |                    round(avg(pumpfrequency) over(PARTITION BY basearea, bgroup, stepno6), 1)
         |                    else null end as xsix_avg_pumpfrequency,            -- 放肩干泵频率均值
         |    case when stepno6 >0 then
         |                    round(avg(seedrotation) over(PARTITION BY basearea, bgroup, stepno6), 1)
         |                    else null end as xsix_avg_srotation,            -- 放肩晶转均值
         |    case when stepno6 >0 then
         |                    last_value(crystallength) over(PARTITION BY basearea, bgroup, stepno6 order by nowtime
         |                    rows between unbounded preceding and unbounded following)
         |                    else null end as xsix_end_crystallength,        -- 放肩终止长度
         |    case when stepno6 >0 then
         |                    last_value(diameter) over(PARTITION BY basearea, bgroup, stepno6 order by nowtime
         |                    rows between unbounded preceding and unbounded following)
         |                    else null end as xsix_end_diameter,            -- 放肩终止直径
         |    case when stepno6 >0 then
         |                    first_value(diameter) over(PARTITION BY basearea, bgroup, stepno6 order by nowtime)
         |                    else null end as xsix_initial_diameter,        -- 放肩起始直径
         |    case when stepno6 >0 then
         |         first_value(meltlevel) over(PARTITION BY basearea, bgroup, stepno6 order by nowtime)
         |         else null end as xsix_initial_meltlevel,        -- 放肩起始液口距
         |    case when (stepno6 >0) and (crystallength <= 20) then
         |                    1 else 0 end as six_rear20_group,                  -- 放肩长度<20mm
         |    case when (stepno6 >0) and (crystallength <= 40) and (crystallength > 20) then
         |                    1 else 0 end as six_rear40_group,                  -- 放肩长度<40mm
         |    case when (stepno6 >0) and (crystallength <= 60) and (crystallength > 40) then
         |                    1 else 0 end as six_rear60_group,                  -- 放肩长度<60mm
         |    case when (stepno6 >0) and (crystallength <= 80) and (crystallength > 60) then
         |                    1 else 0 end as six_rear80_group,                  -- 放肩长度<80mm
         |    case when (stepno6 >0) and (crystallength <= 100) and (crystallength > 80) then
         |                    1 else 0 end as six_rear100_group,                  -- 放肩长度<100mm
         |    case when (stepno6 >0) and (crystallength <= 120) and (crystallength > 100) then
         |                    1 else 0 end as six_rear120_group,                  -- 放肩长度<120mm
         |    case when (stepno6 >0) and (crystallength <= 140) and (crystallength > 120) then
         |                    1 else 0 end as six_rear140_group,                  -- 放肩长度<140mm
         |    case when (stepno6 >0) and (crystallength <= 160) and (crystallength > 140) then
         |                    1 else 0 end as six_rear160_group,                  -- 放肩长度<160mm
         |    case when (stepno6 >0) and (crystallength <= 180) and (crystallength > 160) then
         |                    1 else 0 end as six_rear180_group,                  -- 放肩长度<180mm
         |    case when (stepno6 >0) and (crystallength <= 200)  and (crystallength > 180) then
         |                    1 else 0 end as six_rear200_group,                  -- 放肩长度<200mm
         |    case when stepno6 >0 then
         |         round(abs(first_value(setmainheater) over(PARTITION BY basearea, bgroup, stepno6 order by nowtime)
         |         - last_value(setmainheater) over(PARTITION BY basearea, bgroup, stepno6 order by nowtime
         |         rows between unbounded preceding and unbounded following)), 1)
         |         else null end as xsix_smheater_decay,                -- 放肩设定主加功率降
         |
         |    -- 2022-11-8 add 放肩指标 （亮度偏差）
         |    case when stepno6 >0 then round(meltsurftemp - setmeltsurftemp, 1) else null end as xsix_temp_diff
         |    , case when stepno6>0 and diameter>100 then 1 else 0 end as xsix_temp_group   -- 放肩直径100mm时亮度偏差分组
         |
         |    -- !转肩指标提取! --
         |    , case when stepno7 >0 then min(nowtime) over(PARTITION BY basearea, bgroup, stepno7)
         |         else null end as xsev_starttime,        -- 转肩起始时刻
         |    case when stepno7 >0 then first_value(diameter) over(PARTITION BY basearea, bgroup, stepno7 order by nowtime)
         |         else null end as xsev_initial_diameter,        -- 转肩起始直径
         |    case when stepno7 >0 then
         |         round((unix_timestamp(max(nowtime) over(PARTITION BY basearea, bgroup, stepno7))
         |         - unix_timestamp(min(nowtime) over(PARTITION BY basearea, bgroup, stepno7)))/60, 1)
         |         else null end as xsev_duration,         -- 转肩持续时长
         |    case when stepno7 >0 then
         |         max(avgslspeed) over(PARTITION BY basearea, bgroup, stepno7)
         |         else null end as xsev_max_avgslspeed,        -- 转肩最大拉速
         |    case when stepno7 >0 then
         |         round(abs(first_value(setmainheater) over(PARTITION BY basearea, bgroup, stepno7 order by nowtime)
         |         - last_value(setmainheater) over(PARTITION BY basearea, bgroup, stepno7 order by nowtime
         |         rows between unbounded preceding and unbounded following)), 1)
         |         else null end as xsev_diff_setmheater,
         |    row_number() over(partition by basearea, bgroup, stepno7 order by nowtime) rank7
         |from (
         |       ----- 不要改动
         |       select *,
         |           unix_timestamp(nowtime) - unix_timestamp(lasttime) as difftime,    -- 时间差
         |           round(meltsurftemp - setmeltsurftemp, 1) as diffsurftemp,          --亮度差
         |           case when state = 8 then MEGP else 0 end stepno8,   -- 等径
         |           case when state = 9 then ENGP else 0 end stepno9,   -- 收尾
         |           case when state = 7 then TUGP else 0 end stepno7,   -- 转肩
         |           case when state = 6 then SHGP else 0 end stepno6,   -- 放肩
         |           case when state = 5 then SEGP else 0 end stepno5,   -- 引晶
         |           case when state = 4 then TEGP else 0 end stepno4,   -- 调温
         |           case when TEGP = 0 then CBGP else 0 end stepno3,    -- 预调温
         |           case when (state != 3) and (CBGP=0) then bgroup else 0 end stepno2  -- 化料（不包含熔料）
         |           , case when state = 3 then 1 else null end as xishead    -- 是否首段
         |       from first_layer_table
         |       --- 不要改动
         |   ) xfirst_layer_table
         |where stepno2 > 0 or stepno3 >0 or stepno4 >0
         |   or stepno5 >0 or stepno6 >0 or stepno7 >0
         |   or stepno8 >0 or stepno9 >0
      """.stripMargin)
    temp_df01.createOrReplaceTempView("second_layer_table")
    //    temp_df01.where("stepno8>0").show(10)

    // 上面应该没有问题-三次指标组合
    val temp_df02 = spark.sql(
      s"""
         |select basearea,
         |        base,
         |        AREA,
         |        puller,
         |        crystalid,
         |        nday,
         |        nowtime,
         |        state,
         |        diameter,
         |        bottomheater,
         |        mainheater,
         |        setmainheater,
         |        setbottomheater,
         |        setdiameter,
         |        meltsurftemp,
         |        setmeltsurftemp,
         |        avgslspeed,
         |        setslspeed,
         |        meltlevel,
         |        setmeltlevel,
         |        crystallength,
         |        cruciblepos,
         |        crystalpos,
         |        runduration,
         |        feedervalveopen,
         |        isolationvalveopen,
         |        TEGP,
         |        SEGP,
         |        SHGP,
         |        TUGP,
         |        MEGP,
         |        ENGP,
         |        CBGP,
         |        BGROUP,
         |        difftime,
         |        diffsurftemp,
         |        stepno8,
         |        stepno9,
         |        stepno7,
         |        stepno6,
         |        stepno5,
         |        stepno4,
         |        stepno3,
         |        stepno2,
         |        xeig_starttime,
         |        xeig_duration,
         |        xeig_initial_cposition,
         |        xeig_end_cposition,
         |        xeig_min_mheater,
         |        xeig_max_mheater,
         |        xeig_initial_sdiameter,
         |        xeig_avg_bheater,
         |        xeig_min_meltlevel,
         |        xeig_end_meltlevel,
         |        xeig_end_crystallength,
         |        xeig_avg_mpressure,
         |        xeig_avg_argonflow,
         |        xeig_avg_pumpfrequency,
         |        xeig_end_reweight,
         |        xeig_front500_clift,
         |        xeig_rear_10clength,
         |        xeig_front500_nt,
         |        xeig_mid1200_nt,
         |        xeig_front150_diameter,
         |        xfront150_setdiameter,
         |        xeig_front5_diameter,
         |        xeig_avgsmeltlevel,
         |        xeig_entirety_avgspeed,
         |        xnin_starttime,
         |        xnin_duration,
         |        xnin_end_reweight,
         |        xnin_tail_weight,
         |        xnin_tail_length,
         |        rank9,
         |        fishead,
         |        xtwo_starttime,
         |        xtwo_duration,
         |        xtwo_initial_reweight,
         |        xtwo_end_reweight,
         |        xtwo_avg_mpressure,
         |        xtwo_min_mpressure,
         |        xtwo_max_mpressure,
         |        xtwo_std_mpressure,
         |        xtwo_avg_mheater,
         |        xtwo_avg_bheater,
         |        xtwo_end_cposition,
         |        xtwo_last_5crotation,
         |        open_nt,
         |        xtwo_max_setmheater,
         |        xtwo_max_setbheater,
         |        xtwo_avg_argonflow,
         |        xthr_starttime,
         |        xthr_duration,
         |        xthr_mheater_area,
         |        xthr_bheater_area,
         |        xthr_end_reweight,
         |        xthr_end_cposition,
         |        xthr_last_5crotation,
         |        xthr_last_5mpressure,
         |        xthr_last_5meltlevel,
         |        xthr_surftemp_area,
         |        xthr_max_diffsurftemp,
         |        xthr_min_diffsurftemp,
         |        xthr_std_diffsurftemp,
         |        xthr_mark,
         |        xfou_starttime,
         |        xfou_duration,
         |        xfou_surftemp_area,
         |        xfou_min_smheater,
         |        xfou_initial_mheater,
         |        xfou_avg_bheater,
         |        xfou_avg_meltlevel,
         |        xfou_avg_crotation,
         |        xfou_avg_cposition,
         |        xfou_avg_mpressure,
         |        xfou_avg_argonflow,
         |        xfou_min_crystalpos,
         |        xfou_start_heatexchangerpos,
         |        xfou_last_setmheater,
         |        xfou_avg_diffsurftemp,
         |        xfou_min_diffsurftemp,
         |        xfou_max_diffsurftemp,
         |        xfou_std_diffsurftemp,
         |        xfou_initial_diffsurftemp,
         |        xfiv_starttime,
         |        xfiv_duration,
         |        xfiv_avg_mheater,
         |        xfiv_avg_bheater,
         |        xfiv_avg_meltlevel,
         |        xfiv_avg_crotation,
         |        xfiv_avg_cposition,
         |        xfiv_avg_mpressure,
         |        xfiv_avg_argonflow,
         |        xfiv_avg_pumpfrequency,
         |        xfiv_avg_srotation,
         |        xfiv_end_crystallength,
         |        xfiv_rear100_diameter,
         |        xfiv_diffsurftemp_area,
         |        xfiv_extre_diffsurftemp,
         |        xfiv_avg_diffsurftemp,
         |        xfiv_std_diffsurftemp,
         |        xfiv_lt6_clength,
         |        xfiv_diameter0,
         |        xfiv_diameterB,
         |        xsix_starttime,
         |        xsix_duration,
         |        xsix_avg_mheater,
         |        xsix_avg_bheater,
         |        xsix_avg_meltlevel,
         |        xsix_avg_crotation,
         |        xsix_avg_cposition,
         |        xsix_avg_mpressure,
         |        xsix_avg_argonflow,
         |        xsix_avg_pumpfrequency,
         |        xsix_avg_srotation,
         |        xsix_end_crystallength,
         |        xsix_end_diameter,
         |        xsix_initial_diameter,
         |        xsix_initial_meltlevel,
         |        xsix_smheater_decay,
         |        xsev_starttime,
         |        xsev_initial_diameter,
         |        xsev_duration,
         |        xsev_max_avgslspeed,
         |        xsev_diff_setmheater,
         |        rank7,
         |         -- * 等径二次组合指标 * --
         |         round(avg(xeig_front300_crotation) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_front300_avgcrotation,   -- 等径前300mm埚转均值
         |         round(avg(xeig_rear_30crotation) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_rear30_avgcrotation,       -- 等径后30min埚转均值
         |         round(avg(xeig_front_30srotation) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_front30_avgsrotation,       -- 等径后30min晶转均值
         |         round(avg(xeig_rear_30srotation) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_rear30_avgsrotation,       -- 等径后30min晶转均值
         |         round(avg(xeig_front5_diameter) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_front5_avgdiameter,       -- 等径前5mm直径均值
         |         round(avg(xeig_local100_clift) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local100_avgclift,         -- 等径0-100mm埚升均值
         |         round(avg(xeig_local200_clift) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local200_avgclift,         -- 等径100-200mm埚升均值
         |         round(avg(xeig_local300_clift) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local300_avgclift,         -- 等径200-300mm埚升均值
         |         round(avg(xeig_local400_clift) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local400_avgclift,         -- 等径300-400mm埚升均值
         |         round(avg(xeig_local500_clift) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local500_avgclift,         -- 等径400-500mm埚升均值
         |         --- * 2023-04-24 等径瞬时主加功率 * ---
         |         case when dj_group = 1 and stepno8 > 0 THEN first_value(mainheater) over(PARTITION BY basearea, bgroup, stepno8, dj_group order by nowtime desc) ELSE null END as xeig_local100_mheater,
         |         case when dj_group = 2 and stepno8 > 0 THEN first_value(mainheater) over(PARTITION BY basearea, bgroup, stepno8, dj_group order by nowtime desc) ELSE null END as xeig_local200_mheater,
         |         case when dj_group = 3 and stepno8 > 0 THEN first_value(mainheater) over(PARTITION BY basearea, bgroup, stepno8, dj_group order by nowtime desc) ELSE null END as xeig_local300_mheater,
         |         case when dj_group = 4 and stepno8 > 0 THEN first_value(mainheater) over(PARTITION BY basearea, bgroup, stepno8, dj_group order by nowtime desc) ELSE null END as xeig_local400_mheater,
         |         case when dj_group = 5 and stepno8 > 0 THEN first_value(mainheater) over(PARTITION BY basearea, bgroup, stepno8, dj_group order by nowtime desc) ELSE null END as xeig_local500_mheater,
         |         case when dj_group = 6 and stepno8 > 0 THEN first_value(mainheater) over(PARTITION BY basearea, bgroup, stepno8, dj_group order by nowtime desc) ELSE null END as xeig_local1000_mheater,
         |         case when dj_group = 7 and stepno8 > 0 THEN first_value(mainheater) over(PARTITION BY basearea, bgroup, stepno8, dj_group order by nowtime desc) ELSE null END as xeig_local1500_mheater,
         |         case when dj_group = 8 and stepno8 > 0 THEN first_value(mainheater) over(PARTITION BY basearea, bgroup, stepno8, dj_group order by nowtime desc) ELSE null END as xeig_local2000_mheater,
         |         case when dj_group = 9 and stepno8 > 0 THEN first_value(mainheater) over(PARTITION BY basearea, bgroup, stepno8, dj_group order by nowtime desc) ELSE null END as xeig_local2500_mheater,
         |         case when dj_group = 10 and stepno8 > 0 THEN first_value(mainheater) over(PARTITION BY basearea, bgroup, stepno8, dj_group order by nowtime desc) ELSE null END as xeig_local3000_mheater,
         |         case when dj_group = 11 and stepno8 > 0 THEN first_value(mainheater) over(PARTITION BY basearea, bgroup, stepno8, dj_group order by nowtime desc) ELSE null END as xeig_local3500_mheater,
         |
         |         -- ！2022-07-22新增（开始）！--
         |         round(avg(xeig_local500_meltlevel) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local500_avgmeltlevel,
         |         round(avg(xeig_local1000_meltlevel) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local1000_avgmeltlevel,
         |         round(avg(xeig_local1500_meltlevel) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local1500_avgmeltlevel,
         |         min(xeig_lt1_clength) over(PARTITION BY basearea, bgroup, stepno8) as xeig_meltlevellt1_clength,
         |         -- PLC拉速均值--
         |         round(avg(xeig_local100_speed) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local100_avgspeed,
         |         round(avg(xeig_local200_speed) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local200_avgspeed,
         |         round(avg(xeig_local300_speed) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local300_avgspeed,
         |         round(avg(xeig_local400_speed) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local400_avgspeed,
         |         round(avg(xeig_local500_speed) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local500_avgspeed,
         |         round(avg(xeig_local1200_speed) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local1200_avgspeed,
         |         round(avg(xeig_local1500_setspeed) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local1500_avgsetspeed,
         |         -- PLC拉速标准差--
         |         round(stddev(xeig_local100_speed) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local100_stdspeed,
         |         round(stddev(xeig_local200_speed) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local200_stdspeed,
         |         round(stddev(xeig_local300_speed) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local300_stdspeed,
         |         round(stddev(xeig_local400_speed) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local400_stdspeed,
         |         round(stddev(xeig_local500_speed) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local500_stdspeed,
         |         round(stddev(xeig_local1200_speed) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local1200_stdspeed,
         |         -- PLC拉速变差标准差--
         |         round(stddev(xeig_local100_diffspeed) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local100_stddiffspeed,
         |         round(stddev(xeig_local200_diffspeed) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local200_stddiffspeed,
         |         round(stddev(xeig_local300_diffspeed) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local300_stddiffspeed,
         |         round(stddev(xeig_local400_diffspeed) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local400_stddiffspeed,
         |         round(stddev(xeig_local500_diffspeed) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local500_stddiffspeed,
         |         round(stddev(xeig_local1200_diffspeed) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local1200_stddiffspeed,
         |         -- 直径标准差--
         |         round(stddev(xeig_local100_diameter) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local100_stddiameter,
         |         round(stddev(xeig_local200_diameter) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local200_stddiameter,
         |         round(stddev(xeig_local300_diameter) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local300_stddiameter,
         |         round(stddev(xeig_local400_diameter) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local400_stddiameter,
         |         round(stddev(xeig_local500_diameter) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local500_stddiameter,
         |         round(stddev(xeig_local1200_diameter) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local1200_stddiameter,
         |         -- 直径偏差标准差--
         |         round(stddev(xeig_local100_diffdiameter) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local100_stddiffdiameter,
         |         round(stddev(xeig_local200_diffdiameter) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local200_stddiffdiameter,
         |         round(stddev(xeig_local300_diffdiameter) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local300_stddiffdiameter,
         |         round(stddev(xeig_local400_diffdiameter) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local400_stddiffdiameter,
         |         round(stddev(xeig_local500_diffdiameter) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local500_stddiffdiameter,
         |         round(stddev(xeig_local1200_diffdiameter) over(PARTITION BY basearea, bgroup, stepno8), 1) as xeig_local1200_stddiffdiameter,
         |         -- ！2022-07-22新增（结束）！--
         |
         |         round((max(xeig_front150_diameter) over(PARTITION BY basearea, bgroup, stepno8)
         |             - avg(xfront150_setdiameter) over(PARTITION BY basearea, bgroup, stepno8)), 1)
         |             as xeig_front150_maxdiff_diameter,   -- 等径前150mm最大极差
         |         round((min(xeig_front150_diameter) over(PARTITION BY basearea, bgroup, stepno8)
         |             - avg(xfront150_setdiameter) over(PARTITION BY basearea, bgroup, stepno8)), 1)
         |             as xeig_front150_mindiff_diameter,   -- 等径前150mm最小极差
         |         round((max(xeig_front500_clength) over(PARTITION BY basearea, bgroup, stepno8)/
         |             ((unix_timestamp(max(xeig_front500_nt) over(PARTITION BY basearea, bgroup, stepno8))
         |             - unix_timestamp(min(xeig_front500_nt) over(PARTITION BY basearea, bgroup, stepno8)))/3600)), 1)
         |             as xeig_front500_avgspeed,  -- 等径前500mm平均拉速
         |         round(((max(xeig_mid1200_clength) over(PARTITION BY basearea, bgroup, stepno8)
         |             - min(xeig_mid1200_clength) over(PARTITION BY basearea, bgroup, stepno8))/
         |             ((unix_timestamp(max(xeig_mid1200_nt) over(PARTITION BY basearea, bgroup, stepno8))
         |             - unix_timestamp(min(xeig_mid1200_nt) over(PARTITION BY basearea, bgroup, stepno8)))/3600)), 1)
         |             as xeig_mid1200_avgspeed,  -- 等径500-1200mm平均拉速
         |         max(xeig_front500_clift) over(PARTITION BY basearea, bgroup, stepno8) as xeig_front500_maxclift   -- 等径前500mm最大埚升
         |
         |         -- add 等径前500mm 最大埚升对应的晶体长度
         |         , case when (abs(max(xeig_front500_clift) over(PARTITION BY basearea, bgroup, stepno8) - xeig_front500_clift) < 0.1)
         |             and (stepno8 >0) then crystallength else null end as xeig_front500_clift_map_clength   -- 等径前500mm 最大埚升对应的晶体长度
         |         ,case when (abs(max(xeig_front500_clift) over(PARTITION BY basearea, bgroup, stepno8) - xeig_front500_clift) < 0.1)
         |             and (stepno8 >0) then xeig_front500_nt else null end as xeig_front500_clift_nt,                            -- 等径500mm最大埚升时间
         |         (xeig_end_crystallength - xeig_rear_10clength)*6 as xeig_rear10_avgspeed,                                       -- 等径结束前10min拉速
         |         max(runduration) over(partition by basearea, bgroup, stepno8) xrunduration,   -- 运行时长
         |
         |         -- * 化料二次组合指标 * --
         |         round(avg(xtwo_last_5crotation) over(PARTITION BY basearea, bgroup, stepno2), 3) as xtwo_avg5_crotation,        -- 化料最后5min埚转
         |         round(sum(xtwo_mheater_area) over(PARTITION BY basearea, bgroup, stepno2), 3) as xtwo_mheater_energy,           -- 化料主加总能量
         |         round(sum(xtwo_bheater_area) over(PARTITION BY basearea, bgroup, stepno2), 3) as xtwo_bheater_energy,           -- 化料底加总能量
         |         round(avg(xquotient_mresistance) over(PARTITION BY basearea, bgroup, stepno2), 1) as xtwo_avg_mresistance,      -- 化料主加电阻均值
         |         round(avg(xquotient_bresistance) over(PARTITION BY basearea, bgroup, stepno2), 1) as xtwo_avg_bresistance,      -- 化料底加电阻均值
         |         round(xtwo_end_reweight - xtwo_initial_reweight, 1) as xtwo_feed_quanitity,                                     -- 化料阶段投料量
         |         round((xtwo_end_reweight - xtwo_initial_reweight)/xtwo_duration, 3) as xtwo_feed_efficiency,                    -- 化料阶段投料效率
         |
         |         -- * add 2022-11-09 新增
         |         max(open_nt) over(PARTITION BY basearea, bgroup, stepno2) as two_max_open_nt,
         |         min(open_nt) over(PARTITION BY basearea, bgroup, stepno2) as two_min_open_nt,
         |
         |         max(nowtime) over(PARTITION BY basearea, bgroup, stepno2) as xtwo_endtime,                                      -- 化料阶段终止时间
         |
         |         -- * 调温准备二次组合指标 * --
         |         round(avg(xthr_last_5crotation) over(PARTITION BY basearea, bgroup, stepno3), 1) as xthr_avg5_crotation,        -- 准备调温最后5min埚转均值
         |         round(avg(xthr_last_5mpressure) over(PARTITION BY basearea, bgroup, stepno3), 1) as xthr_avg5_mpressure,        -- 准备调温最后5min炉压均值
         |         round(avg(xthr_last_5meltlevel) over(PARTITION BY basearea, bgroup, stepno3), 1) as xthr_avg5_meltlevel,        -- 准备调温最后5min液口距均值
         |         round(sum(xthr_mheater_area) over(PARTITION BY basearea, bgroup, stepno3), 2) as xthr_mheater_energy,           -- 准备调温主加总能量
         |         round(sum(xthr_bheater_area) over(PARTITION BY basearea, bgroup, stepno3), 2) as xthr_bheater_energy,           -- 准备调温底加总能量
         |         round(sum(xthr_surftemp_area) over(PARTITION BY basearea, bgroup, stepno3), 2) as xthr_sum_areasurftemp,        -- 准备调温液面亮度和
         |
         |         -- add 2022-11-08 调温准备增加指标
         |         case when stepno3 >0 and xthr_mark = 1 then
         |             min(nowtime) over(PARTITION BY basearea, bgroup, stepno3, xthr_mark)
         |             else null end xthr_gt5_starttime,     -- 埚转>5的起始时
         |
         |         -- * 调温二次组合指标 * --
         |         round(sum(xfou_surftemp_area) over(PARTITION BY basearea, bgroup, stepno4), 3) as xfou_sum_areasurftemp,        -- 调温液面亮度和
         |         case when (abs(setmainheater - xfou_last_setmheater) < 0.1) and (stepno4 >0) then
         |              nowtime else null end as xfou_steady_temp_nt,                                                              -- 调温稳温时间
         |         case when (stepno4 >0) and abs(xfou_min_crystalpos - crystalpos) < 0.1 then
         |              diameter else null end xfou_weld_diameter1,                                                                -- 熔接直径
         |         case when (stepno4 >0) and abs(xfou_min_crystalpos - crystalpos) < 0.1 then
         |              from_unixtime(unix_timestamp(nowtime)+120, 'yyyy-MM-dd HH:mm:ss')
         |              else null end as xfou_cplowestnt,                                                                          -- 籽晶最低点+2min
         |         case when (stepno4 >0) and abs(xfou_min_smheater - setmainheater) < 0.1 then
         |              difftime else null end as xfou_smheater_difftime,                                                          -- 调温最低功率时间长度s
         |
         |         -- * 引晶二次组合指标 * --
         |         round(avg(xfiv_rear100_avgslspeed) over(PARTITION BY basearea, bgroup, stepno5), 1) as xfiv_avg_speed1,        -- 引晶100mm后拉速均值
         |         round(avg(xfiv_rear130_avgslspeed) over(PARTITION BY basearea, bgroup, stepno5), 1) as xfiv_avg_speed2,        -- 引晶130mm后拉速均值
         |         round(avg(xfiv_rear160_avgslspeed) over(PARTITION BY basearea, bgroup, stepno5), 1) as xfiv_avg_speed3,        -- 引晶160mm后拉速均值
         |         round(xfiv_end_crystallength/(xfiv_duration/60), 2) as xfiv_entirety_speed,                                       -- 引晶全程平均拉速
         |         round(avg(xfiv_rear100_diameter) over(PARTITION BY basearea, bgroup, stepno5), 2) as xfiv_rear100_avgdiameter, -- 引晶后100mm直径均值
         |         round(min(xfiv_rear100_diameter) over(PARTITION BY basearea, bgroup, stepno5), 2) as xfiv_rear100_mindiameter, -- 引晶后100mm直径最小值
         |         round(max(xfiv_rear100_diameter) over(PARTITION BY basearea, bgroup, stepno5), 2) as xfiv_rear100_maxdiameter, -- 引晶后100mm直径最大值
         |         round(stddev(xfiv_rear100_diameter) over(PARTITION BY basearea, bgroup, stepno5), 2) as xfiv_rear100_stddiameter, -- 引晶后100mm直径标准差
         |
         |         -- ！2022-07-22（新增）！--
         |         min(xfiv_lt6_clength) over(PARTITION BY basearea, bgroup, stepno5) as xfiv_diameterlt6_clength,
         |
         |         round(sum(xfiv_diffsurftemp_area) over(PARTITION BY basearea, bgroup, stepno5), 1) as xfiv_sum_areadiffsurftemp, -- 引晶液温差面积
         |         case when fiv_mid20_group >0 then
         |               last_value(diameter) over(PARTITION BY basearea, bgroup, stepno5, fiv_mid20_group order by nowtime
         |               rows between unbounded preceding and unbounded following)
         |               else null end as ffiv_diameter1,                                                                          -- 引晶长度20mm刻直径值
         |         case when fiv_mid40_group >0 then
         |               last_value(diameter) over(PARTITION BY basearea, bgroup, stepno5, fiv_mid40_group order by nowtime
         |               rows between unbounded preceding and unbounded following) else null end as ffiv_diameter2,
         |         case when fiv_mid60_group >0 then
         |               last_value(diameter) over(PARTITION BY basearea, bgroup, stepno5, fiv_mid60_group order by nowtime
         |               rows between unbounded preceding and unbounded following) else null end as ffiv_diameter3,
         |         case when fiv_mid80_group >0 then
         |               last_value(diameter) over(PARTITION BY basearea, bgroup, stepno5, fiv_mid80_group order by nowtime
         |               rows between unbounded preceding and unbounded following) else null end as ffiv_diameter4,
         |         case when fiv_mid100_group >0 then
         |               last_value(diameter) over(PARTITION BY basearea, bgroup, stepno5, fiv_mid100_group order by nowtime
         |               rows between unbounded preceding and unbounded following) else null end as ffiv_diameter5,
         |         case when fiv_mid120_group >0 then
         |               last_value(diameter) over(PARTITION BY basearea, bgroup, stepno5, fiv_mid120_group order by nowtime
         |               rows between unbounded preceding and unbounded following) else null end as ffiv_diameter6,
         |         case when fiv_mid140_group >0 then
         |               last_value(diameter) over(PARTITION BY basearea, bgroup, stepno5, fiv_mid140_group order by nowtime
         |               rows between unbounded preceding and unbounded following) else null end as ffiv_diameter7,
         |         case when fiv_mid160_group >0 then
         |               last_value(diameter) over(PARTITION BY basearea, bgroup, stepno5, fiv_mid160_group order by nowtime
         |               rows between unbounded preceding and unbounded following) else null end as ffiv_diameter8,
         |         case when fiv_mid180_group >0 then
         |               last_value(diameter) over(PARTITION BY basearea, bgroup, stepno5, fiv_mid180_group order by nowtime
         |               rows between unbounded preceding and unbounded following) else null end as ffiv_diameter9,
         |         case when fiv_mid200_group >0 then
         |               last_value(diameter) over(PARTITION BY basearea, bgroup, stepno5, fiv_mid200_group order by nowtime
         |               rows between unbounded preceding and unbounded following) else null end as ffiv_diameterA,
         |
         |
         |         -- * 放肩二次组合指标 * --
         |         round(xsix_end_crystallength/(xsix_duration/60), 2) as xsix_avg_speed,                                       -- 放肩全程平均拉速
         |         case when six_rear20_group >0 then
         |               last_value(diameter) over(PARTITION BY basearea, bgroup, stepno6, six_rear20_group order by nowtime
         |               rows between unbounded preceding and unbounded following) else null end as xsix_shard_diameter1,     -- 放肩20mm处直径
         |         case when six_rear40_group >0 then
         |               last_value(diameter) over(PARTITION BY basearea, bgroup, stepno6, six_rear40_group order by nowtime
         |               rows between unbounded preceding and unbounded following) else null end as xsix_shard_diameter2,     -- 放肩40mm处直径
         |         case when six_rear60_group >0 then
         |               last_value(diameter) over(PARTITION BY basearea, bgroup, stepno6, six_rear60_group order by nowtime
         |               rows between unbounded preceding and unbounded following) else null end as xsix_shard_diameter3,     -- 放肩60mm处直径
         |         case when six_rear80_group >0 then
         |               last_value(diameter) over(PARTITION BY basearea, bgroup, stepno6, six_rear80_group order by nowtime
         |               rows between unbounded preceding and unbounded following) else null end as xsix_shard_diameter4,     -- 放肩80mm处直径
         |         case when six_rear100_group >0 then
         |               last_value(diameter) over(PARTITION BY basearea, bgroup, stepno6, six_rear100_group order by nowtime
         |               rows between unbounded preceding and unbounded following) else null end as xsix_shard_diameter5,     -- 放肩100mm处直径
         |         case when six_rear120_group >0 then
         |               last_value(diameter) over(PARTITION BY basearea, bgroup, stepno6, six_rear120_group order by nowtime
         |               rows between unbounded preceding and unbounded following) else null end as xsix_shard_diameter6,     -- 放肩120mm处直径
         |         case when six_rear140_group >0 then
         |               last_value(diameter) over(PARTITION BY basearea, bgroup, stepno6, six_rear140_group order by nowtime
         |               rows between unbounded preceding and unbounded following)
         |               else null end as xsix_shard_diameter7,                                                               -- 放肩140mm处直径
         |         case when six_rear160_group >0 then
         |               last_value(diameter) over(PARTITION BY basearea, bgroup, stepno6, six_rear160_group order by nowtime
         |               rows between unbounded preceding and unbounded following)
         |               else null end as xsix_shard_diameter8,                                                               -- 放肩160mm处直径
         |         case when six_rear180_group >0 then
         |               last_value(diameter) over(PARTITION BY basearea, bgroup, stepno6, six_rear180_group order by nowtime
         |               rows between unbounded preceding and unbounded following)
         |               else null end as xsix_shard_diameter9,                                                               -- 放肩180mm处直径
         |         case when six_rear200_group >0 then
         |               last_value(diameter) over(PARTITION BY basearea, bgroup, stepno6, six_rear200_group order by nowtime
         |               rows between unbounded preceding and unbounded following)
         |               else null end as xsix_shard_diameterA                                                               -- 放肩200mm处直径
         |         -- add 2022-11-08
         |         , first_value(xsix_temp_diff) over(partition by basearea, bgroup, stepno6 order by nowtime) as xsix_init_temp_diff  -- 初始亮度偏差
         |         , case when stepno6>0 and xsix_temp_group=1 then
         |                first_value(xsix_temp_diff) over(partition by basearea, bgroup, stepno6, xsix_temp_group order by nowtime)
         |              else null end as xsix_100_temp_diff   -- 放肩直径100mm时亮度偏差
         | from second_layer_table
         | where (rank7=1 and stepno7>0)
         |      or (stepno2>0)
         |      or (stepno3>0)
         |      or (stepno4>0)
         |      or (stepno5>0)
         |      or (stepno6>0)
         |      or (stepno8>0)
         |      or (rank9=1 and stepno9>0)
       """.stripMargin)

    temp_df02.createOrReplaceTempView("kylin_temp1")
    //    println("-----------test5----------")
    //
    val temp_df03 = spark.sql(
      """
        |select *
        |       -- add 2022-11-08
        |       , case when stepno3>0 and xthr_mark=1 and xthr_diff_smheater != 0
        |            then nowtime else null end thr_smheater_drop_nt  --功率降时间
        | from(select *
        |              -- * add 2022-11-09
        |              , case when stepno3 >0 and xthr_mark = 1 then max(xtwo_open_smheater) over(PARTITION BY basearea, bgroup, stepno2)
        |                   - setmainheater else 0 end as xthr_diff_smheater     -- 埚转>5后设定功率与起始功率差
        |              from (
        |                 select *,
        |                      -- *add 2022-11-09
        |                      case when two_max_open_nt=nowtime then setmainheater else null end as xtwo_open_smheater,           --- 准备调温开始功率
        |                      --！化料四次指标组合！--
        |                      round((unix_timestamp(two_min_open_nt) - unix_timestamp(xtwo_starttime))/60, 1) as xtwo_preheater_duration,         -- 化料预热时间
        |                      round((unix_timestamp(xtwo_endtime) - unix_timestamp(two_max_open_nt))/60, 1)
        |                             as xtwo_last_meltduration,                                                -- 最后一桶化料时间,隔离阀关闭至关底加时间
        |                      case when (stepno2>0) and (two_min_open_nt= nowtime) then
        |                           cruciblepos else null end as xtwo_min_cposition      -- 初始化料埚位
        |                      -- 调温准备阶段
        |                      , round((unix_timestamp(max(xthr_gt5_starttime) over(PARTITION BY basearea, bgroup, stepno3))
        |                             - unix_timestamp(xthr_starttime))/60, 1) as xthr_cbot_to_crotation_gt5_duration    -- 关底加至打埚转时长
        |                      , max(nowtime) over(PARTITION BY basearea, bgroup, stepno3) as thr_endtime,
        |
        |                      -- 引晶肩型
        |                     max(ffiv_diameter1) over(PARTITION BY basearea, bgroup, stepno5) as xfiv_diameter1,
        |                     max(ffiv_diameter2) over(PARTITION BY basearea, bgroup, stepno5) as xfiv_diameter2,
        |                     max(ffiv_diameter3) over(PARTITION BY basearea, bgroup, stepno5) as xfiv_diameter3,
        |                     max(ffiv_diameter4) over(PARTITION BY basearea, bgroup, stepno5) as xfiv_diameter4,
        |                     max(ffiv_diameter5) over(PARTITION BY basearea, bgroup, stepno5) as xfiv_diameter5,
        |                     max(ffiv_diameter6) over(PARTITION BY basearea, bgroup, stepno5) as xfiv_diameter6,
        |                     max(ffiv_diameter7) over(PARTITION BY basearea, bgroup, stepno5) as xfiv_diameter7,
        |                     max(ffiv_diameter8) over(PARTITION BY basearea, bgroup, stepno5) as xfiv_diameter8,
        |                     max(ffiv_diameter9) over(PARTITION BY basearea, bgroup, stepno5) as xfiv_diameter9,
        |                     max(ffiv_diameterA) over(PARTITION BY basearea, bgroup, stepno5) as xfiv_diameterA,
        |                      row_number() over(partition by basearea, bgroup, stepno5 order by nowtime) as rank5,
        |
        |                      -- add 放肩亮度偏差
        |                      max(xsix_init_temp_diff) over(partition by basearea, bgroup, stepno6) as tsix_init_temp_diff,
        |                      max(xsix_100_temp_diff) over(partition by basearea, bgroup, stepno6) as tsix_100_temp_diff,
        |                       max(xsix_shard_diameter1) over(PARTITION BY basearea, bgroup, stepno6) as fsix_diameter1,
        |                       max(xsix_shard_diameter2) over(PARTITION BY basearea, bgroup, stepno6) as fsix_diameter2,
        |                       max(xsix_shard_diameter3) over(PARTITION BY basearea, bgroup, stepno6) as fsix_diameter3,
        |                       max(xsix_shard_diameter4) over(PARTITION BY basearea, bgroup, stepno6) as fsix_diameter4,
        |                       max(xsix_shard_diameter5) over(PARTITION BY basearea, bgroup, stepno6) as fsix_diameter5,
        |                       max(xsix_shard_diameter6) over(PARTITION BY basearea, bgroup, stepno6) as fsix_diameter6,
        |                       max(xsix_shard_diameter7) over(PARTITION BY basearea, bgroup, stepno6) as fsix_diameter7,
        |                       max(xsix_shard_diameter8) over(PARTITION BY basearea, bgroup, stepno6) as fsix_diameter8,
        |                       max(xsix_shard_diameter9) over(PARTITION BY basearea, bgroup, stepno6) as fsix_diameter9,
        |                       max(xsix_shard_diameterA) over(PARTITION BY basearea, bgroup, stepno6) as fsix_diameterA,
        |                       row_number() over(partition by basearea, bgroup, stepno6 order by nowtime) as rank6,
        |                      -- 2023-04-24 增加等径阶段主加功率
        |                      max(xeig_local100_mheater)over(partition by basearea, bgroup, stepno8) as eig_local100_mheater,
        |                      max(xeig_local200_mheater)over(partition by basearea, bgroup, stepno8) as eig_local200_mheater,
        |                      max(xeig_local300_mheater)over(partition by basearea, bgroup, stepno8) as eig_local300_mheater,
        |                      max(xeig_local400_mheater)over(partition by basearea, bgroup, stepno8) as eig_local400_mheater,
        |                      max(xeig_local500_mheater)over(partition by basearea, bgroup, stepno8) as eig_local500_mheater,
        |                      max(xeig_local1000_mheater)over(partition by basearea, bgroup, stepno8) as eig_local1000_mheater,
        |                      max(xeig_local1500_mheater)over(partition by basearea, bgroup, stepno8) as eig_local1500_mheater,
        |                      max(xeig_local2000_mheater)over(partition by basearea, bgroup, stepno8) as eig_local2000_mheater,
        |                      max(xeig_local2500_mheater)over(partition by basearea, bgroup, stepno8) as eig_local2500_mheater,
        |                      max(xeig_local3000_mheater)over(partition by basearea, bgroup, stepno8) as eig_local3000_mheater,
        |                      max(xeig_local3500_mheater)over(partition by basearea, bgroup, stepno8) as eig_local3500_mheater,
        |
        |                      -- add 最大埚升对应晶体长度
        |                      max(xeig_front500_clift_map_clength) over(PARTITION BY basearea, bgroup, stepno8) as teig_front500_clift_map_clength  -- 等径前500mm最大埚升对应的长度
        |                  from kylin_temp1
        |              ) kylin_temp2
        |         ) kylin_temp3
        |         where (stepno8 >0 and xeig_front500_clift_nt is not null)
        |              or (stepno2>0)
        |              or (stepno3>0)
        |              or (stepno4>0)
        |              or (stepno7>0)
        |              or (stepno9>0)
        |              or (stepno6 >0 and rank6=1)
        |              or (stepno5 >0 and rank5=1)
      """.stripMargin)

    temp_df03.createOrReplaceTempView("fourth_layer_table")
    // 四次指标组合
    val temp_df04 = spark.sql(
      s"""
         |SELECT *
         |      , MAX(tthr_crotation_gt5_smheater_drop_duration) over(PARTITION BY basearea, bgroup, stepno3) as xthr_crotation_gt5_smheater_drop_duration
         |      , MAX(tthr_smheater_drop_to_temp_duration) over(PARTITION BY basearea, bgroup, stepno3) as xthr_smheater_drop_to_temp_duration
         |      , MAX(xfou_weld_diameter) over(partition by basearea, bgroup, stepno4) as xfou_weld2_diameter  -- 熔接直径
         |      , MAX(xfou_weld_setmeltsurftemp) over(partition by basearea, bgroup, stepno4) as xfou_weld2_setmeltsurftemp  -- 熔接设定液面亮度
         |      , MAX(xfou_weld_meltsurftemp) over(partition by basearea, bgroup, stepno4) as xfou_weld2_meltsurftemp  -- 熔接液面亮度
         |      , CASE WHEN (xfou_steady_startnt <= nowtime) and (stepno4 >0) then
         |          xfou_weld_diameter else null end as xfou_rear_weld2_diameter     -- 熔接2min后直径值
         |      , CASE WHEN xfou_steady_startnt = nowtime then diameter else null end as xfou_steadytemp_diameter       -- 进入稳温直径
         |      -- add 初始设定功率及偏差
         |      , CASE WHEN (stepno2>0) and (two_max_open_nt = nowtime) then setbottomheater else null end as xtwo_init_sbheater      -- 化料关底加设定底加功率
         |      , CASE WHEN (stepno2>0) and (two_max_open_nt = nowtime) then setmainheater else null end as xtwo_init_smheater        -- 化料关底加设定主加功率
         |      , CASE WHEN (stepno2>0) and (two_max_open_nt = nowtime) then (bottomheater - setbottomheater) else null end as xtwo_init_bheater_diff    -- 化料关底加设定底加功率偏差
         |      , CASE WHEN (stepno2>0) and (two_max_open_nt = nowtime) then (mainheater - setmainheater) else null end as xtwo_init_mheater_diff        -- 化料关底加设定主加功率偏差
         |      , ROW_NUMBER() OVER(PARTITION BY basearea, bgroup, stepno2 order by nowtime) as rank2
         |  FROM (SELECT *
         |              , ROUND((unix_timestamp(max(xeig_front500_clift_nt) over(PARTITION BY basearea, bgroup, stepno8))
         |                   - unix_timestamp(min(xeig_front500_clift_nt) over(PARTITION BY basearea, bgroup, stepno8)))/60, 1) as xeig_front500_maxclift_duration
         |              , ROW_NUMBER() OVER(partition by basearea, bgroup, stepno8 order by nowtime) as rank8    -- 等径排序
         |              -- add 2022-11-09 新增
         |              , ROUND((unix_timestamp(min(thr_smheater_drop_nt) over(PARTITION BY basearea, bgroup, stepno3))
         |                           - unix_timestamp(xthr_gt5_starttime))/60, 1)
         |                     as tthr_crotation_gt5_smheater_drop_duration
         |              , ROUND((unix_timestamp(thr_endtime) - unix_timestamp(min(thr_smheater_drop_nt) over(PARTITION BY basearea, bgroup, stepno3)))/60, 1)
         |                    as tthr_smheater_drop_to_temp_duration
         |              , ROW_NUMBER() over(partition by basearea, bgroup, stepno3 order by nowtime) rank3   -- 准备调温排序
         |              --！调温三次指标组合！--
         |              , MIN(xfou_weld_diameter1) over(PARTITION BY basearea, bgroup, stepno4) as xfou_weld_initial_diameter     -- 熔接起始直径
         |              , CASE WHEN (nowtime > min(xfou_cplowestnt) over(PARTITION BY basearea, bgroup, stepno4)) and (stepno4 >0) then
         |                    diameter else null end as xfou_weld_diameter  -- 调温熔接直径
         |              , CASE WHEN (nowtime > min(xfou_cplowestnt) over(PARTITION BY basearea, bgroup, stepno4)) and (stepno4 >0) then
         |                       setmeltsurftemp else null end as xfou_weld_setmeltsurftemp  -- 调温设定亮度
         |              , CASE WHEN (nowtime > min(xfou_cplowestnt) over(PARTITION BY basearea, bgroup, stepno4)) and (stepno4 >0) then
         |                       meltsurftemp else null end as xfou_weld_meltsurftemp  -- 调温亮度
         |              , ROUND(sum(xfou_smheater_difftime) over(partition by basearea, bgroup, stepno4) / 60, 1)
         |                       as xfou_minsmheater_duration     -- 调温最低设定主加功率维持时长(分钟)
         |              , ROUND((unix_timestamp(max(xfou_steady_temp_nt) over(partition by basearea, bgroup, stepno4))
         |                       - unix_timestamp(min(xfou_steady_temp_nt) over(partition by basearea, bgroup, stepno4)))/60, 1)
         |                       as xfou_steady_duration   -- 调温稳温时长
         |              , MIN(xfou_steady_temp_nt) over(partition by basearea, bgroup, stepno4) as xfou_steady_startnt      -- 调温稳温起始时刻
         |         from fourth_layer_table
         |      ) fifth_layer_table
         |   WHERE (rank8 = 1 and stepno8 >0)
         |       or (stepno2 > 0)
         |       or (stepno4 > 0)
         |       or (stepno5 > 0)
         |       or (stepno6 > 0)
         |       or (stepno7 > 0)
         |       or (stepno9 > 0)
         |       or (rank3 = 1 and stepno3 >0)
       """.stripMargin)
    //    temp_df04.where("stepno8>0").show(10)
    temp_df04.createOrReplaceTempView("kylin_temp2")

    val temp_df05 = spark.sql(
      """
        |select *,
        |   round(avg(xfou_rear_weld2_diameter) over(partition by basearea, bgroup, stepno4), 1) as xfou_weld2_avgdiameter, -- 调温熔接2min后直径均值
        |   -- 新增2022-07-22 --
        |   max(xfou_steadytemp_diameter) over(partition by basearea, bgroup, stepno4) as xfou_steadyfirst_diameter,  -- 进入稳温直径
        |   row_number() over(partition by basearea, bgroup, stepno4 order by nowtime) as rank4
        |   from (select *
        |               , max(xtwo_min_cposition) over(partition by basearea, bgroup, stepno2) as ttwo_min_cposition
        |               , max(xtwo_init_sbheater) over(partition by basearea, bgroup, stepno2) as ttwo_init_sbheater
        |               , max(xtwo_init_smheater) over(partition by basearea, bgroup, stepno2) as ttwo_init_smheater
        |               , max(xtwo_init_bheater_diff) over(partition by basearea, bgroup, stepno2) as ttwo_init_bheater_diff
        |               , max(xtwo_init_mheater_diff) over(partition by basearea, bgroup, stepno2) as ttwo_init_mheater_diff
        |         from kylin_temp2
        |         ) mid_temp
        |         where (stepno2 >0 and rank2=1)
        |           or (stepno3>0)
        |           or (stepno4>0)
        |           or (stepno5>0)
        |           or (stepno6>0)
        |           or (stepno7>0)
        |           or (stepno8>0)
        |           or (stepno9>0)
      """.stripMargin)

    temp_df05.createOrReplaceTempView("sixth_layer_table")

    // 五次指标组合
    val kylin_table = spark.sql(
      s"""
         | select basearea,
         |       base,
         |       AREA,
         |       puller,
         |       crystalid,
         |       state,
         |       iste,
         |       isse,
         |       issh,
         |       istu,
         |       isme,
         |       isen,
         |       iscb,
         |       isob,
         |       runduration,
         |       two_starttime,
         |       two_duration,
         |       two_initial_reweight,
         |       two_end_reweight,
         |       two_avg_mpressure,
         |       two_min_mpressure,
         |       two_max_mpressure,
         |       two_std_mpressure,
         |       two_avg_mheater,
         |       two_avg_bheater,
         |       two_end_cposition,
         |       two_avg5_crotation,
         |       two_mheater_energy,
         |       two_bheater_energy,
         |       two_avg_mresistance,
         |       two_avg_bresistance,
         |       two_feed_quanitity,
         |       two_feed_efficiency,
         |       two_preheater_duration,
         |       two_last_meltduration,    -- 最后一桶熔料时间
         |       two_start_cposition,
         |       thr_starttime,
         |       thr_duration,
         |       thr_end_reweight,
         |       thr_end_cposition,
         |       thr_max_diffsurftemp,
         |       thr_min_diffsurftemp,
         |       thr_std_diffsurftemp,
         |       thr_avg5_crotation,
         |       thr_avg5_mpressure,
         |       thr_avg5_meltlevel,
         |       thr_mheater_energy,
         |       thr_bheater_energy,
         |       thr_sum_areasurftemp,
         |       fou_starttime,
         |       fou_duration,
         |       fou_min_smheater,
         |       fou_initial_mheater,
         |       fou_avg_bheater,
         |       fou_avg_meltlevel,
         |       fou_avg_crotation,
         |       fou_avg_cposition,
         |       fou_avg_mpressure,
         |       fou_avg_argonflow,
         |       fou_min_crystalpos,
         |       fou_start_heatexchangerpos,
         |       fou_last_setmheater,
         |       fou_avg_diffsurftemp,
         |       fou_min_diffsurftemp,
         |       fou_max_diffsurftemp,
         |       fou_std_diffsurftemp,
         |       fou_initial_diffsurftemp,
         |       fou_sum_areasurftemp,
         |       fou_minsmheater_duration,
         |       fou_steady_duration,
         |       fou_weld2_diameter,
         |       fou_weld2_setmeltsurftemp,
         |       fou_weld2_meltsurftemp,
         |       fou_weld2_avgdiameter,
         |       fiv_starttime,
         |       fiv_duration,
         |       fiv_avg_mheater,
         |       fiv_avg_bheater,
         |       fiv_avg_meltlevel,
         |       fiv_avg_crotation,
         |       fiv_avg_cposition,
         |       fiv_avg_mpressure,
         |       fiv_avg_argonflow,
         |       fiv_avg_pumpfrequency,
         |       fiv_avg_srotation,
         |       fiv_end_crystallength,
         |       fiv_sum_areadiffsurftemp,
         |       fiv_extre_diffsurftemp,
         |       fiv_avg_diffsurftemp,
         |       fiv_std_diffsurftemp,
         |       fiv_avg_speed1,
         |       fiv_avg_speed2,
         |       fiv_avg_speed3,
         |       fiv_entirety_speed,
         |       fiv_rear100_avgdiameter,
         |       fiv_rear100_mindiameter,
         |       fiv_rear100_maxdiameter,
         |       fiv_rear100_stddiameter,
         |       six_starttime,
         |       six_duration,
         |       six_avg_mheater,
         |       six_avg_bheater,
         |       six_avg_meltlevel,
         |       six_avg_crotation,
         |       six_avg_cposition,
         |       six_avg_mpressure,
         |       six_avg_argonflow,
         |       six_avg_pumpfrequency,
         |       six_avg_srotation,
         |       six_end_crystallength,
         |       six_end_diameter,
         |       six_initial_diameter,
         |       six_smheater_decay,
         |       six_avg_speed,
         |       six_diameter1,
         |       six_diameter2,
         |       six_diameter3,
         |       six_diameter4,
         |       six_diameter5,
         |       six_diameter6,
         |       six_diameter7,
         |       six_diameter8,
         |       six_diameter9,
         |       six_diameterA,
         |       sev_starttime,
         |       sev_duration,
         |       sev_max_avgslspeed,
         |       sev_diff_setmheater,
         |       eig_starttime,
         |       eig_duration,
         |       eig_initial_cposition,
         |       eig_end_cposition,
         |       eig_min_mheater,
         |       eig_max_mheater,
         |       eig_avg_bheater,
         |       eig_min_meltlevel,
         |       eig_end_meltlevel,
         |       eig_end_crystallength,
         |       eig_avg_mpressure,
         |       eig_avg_argonflow,
         |       eig_avg_pumpfrequency,
         |       eig_end_reweight,
         |       eig_front300_avgcrotation,
         |       eig_rear30_avgcrotation,
         |       eig_front30_avgsrotation,
         |       eig_rear30_avgsrotation,
         |       eig_front5_avgdiameter,
         |       eig_front150_maxdiff_diameter,
         |       eig_front150_mindiff_diameter,
         |       eig_front500_avgspeed,
         |       eig_mid1200_avgspeed,
         |       eig_front500_maxclift,
         |       eig_front500_maxclift_duration,
         |       nin_starttime,
         |       nin_duration,
         |       nin_end_reweight,
         |       nin_tail_weight,
         |       nin_tail_length,
         |       eig_local100_avgclift,
         |       eig_local200_avgclift,
         |       eig_local300_avgclift,
         |       eig_local400_avgclift,
         |       eig_local500_avgclift,
         |       ishead,
         |       fou_weld_initial_diameter,      -- 新增
         |       fiv_start_diameter,             -- 新增
         |       fiv_end_diameter,               -- 新增
         |       fiv_diameter1, -- 新增
         |       fiv_diameter2, -- 新增
         |       fiv_diameter3, -- 新增
         |       fiv_diameter4, -- 新增
         |       fiv_diameter5, -- 新增
         |       fiv_diameter6, -- 新增
         |       fiv_diameter7, -- 新增
         |       fiv_diameter8, -- 新增
         |       fiv_diameter9, -- 新增
         |       fiv_diameterA, -- 新增
         |       six_initial_meltlevel,          -- 新增
         |       sev_initial_diameter,           -- 新增
         |       eig_initial_setdiameter,
         |       --! 2022-07-22(新增)！--
         |       two_max_setmheater,
         |       two_max_setbheater,
         |       two_avg_argonflow,
         |       fou_steadyfirst_diameter,
         |
         |       fiv_diameterlt6_clength,
         |       eig_avgsmeltlevel,
         |       eig_entirety_avgspeed,
         |       eig_local500_avgmeltlevel,
         |       eig_local1000_avgmeltlevel,
         |       eig_local1500_avgmeltlevel,
         |       eig_meltlevellt1_clength,
         |       eig_local100_avgspeed,
         |       eig_local200_avgspeed,
         |       eig_local300_avgspeed,
         |       eig_local400_avgspeed,
         |       eig_local500_avgspeed,
         |       eig_local1500_avgsetspeed,
         |       eig_local100_stdspeed,
         |       eig_local200_stdspeed,
         |       eig_local300_stdspeed,
         |       eig_local400_stdspeed,
         |       eig_local500_stdspeed,
         |       eig_local1200_stdspeed,
         |       eig_local100_stddiffspeed,
         |       eig_local200_stddiffspeed,
         |       eig_local300_stddiffspeed,
         |       eig_local400_stddiffspeed,
         |       eig_local500_stddiffspeed,
         |       eig_local1200_stddiffspeed,
         |       eig_local100_stddiameter,
         |       eig_local200_stddiameter,
         |       eig_local300_stddiameter,
         |       eig_local400_stddiameter,
         |       eig_local500_stddiameter,
         |       eig_local1200_stddiameter,
         |       eig_local100_stddiffdiameter,
         |       eig_local200_stddiffdiameter,
         |       eig_local300_stddiffdiameter,
         |       eig_local400_stddiffdiameter,
         |       eig_local500_stddiffdiameter,
         |       eig_local1200_stddiffdiameter,
         |       eig_local1200_avgspeed,
         |       -- add 2022-11-08
         |       two_init_sbheater,   -- 化料初始设定底加功率
         |       two_init_smheater,   -- 化料初始设定主加功率
         |       two_init_bheater_diff,  -- 化料初始底加偏差
         |       two_init_mheater_diff,   -- 化料初始主加偏差
         |       thr_cbot_to_crotation_gt5_duration,  -- 关底加至埚转>5时长
         |       thr_crotation_gt5_smheater_drop_duration,   -- 埚转>5时长至功率降时长
         |       thr_smheater_drop_to_temp_duration,  -- 功率降至点调温时长
         |       six_init_temp_diff,   -- 刚进入放肩亮度差
         |       six_100_temp_diff,    -- 进入放肩100mm亮度差
         |       eig_front500_clift_map_clength,    -- 等径前500mm最大埚升对应晶棒长度
         |       --- add 2023-04-24
         |       eig_end100_mheater,   --- 等径100mm时刻主加功率
         |       eig_end200_mheater,   --- 等径200mm时刻主加功率
         |       eig_end300_mheater,   --- 等径300mm时刻主加功率
         |       eig_end400_mheater,   --- 等径400mm时刻主加功率
         |       eig_end500_mheater,   --- 等径500mm时刻主加功率
         |       eig_end1000_mheater,  --- 等径1000mm时刻主加功率
         |       eig_end1500_mheater,  --- 等径1500mm时刻主加功率
         |       eig_end2000_mheater,  --- 等径2000mm时刻主加功率
         |       eig_end2500_mheater,  --- 等径2500mm时刻主加功率
         |       eig_end3000_mheater,  --- 等径3000mm时刻主加功率
         |       eig_end3500_mheater,  --- 等径3500mm时刻主加功率
         |       left(two_starttime, 10) as day_id --开底加分区
         | from (select basearea,
         |                     base,
         |                     AREA,
         |                     puller,
         |                     crystalid,
         |                     nowtime,
         |                     state,
         |                     TEGP as iste,
         |                     SEGP as isse,
         |                     SHGP as issh,
         |                     TUGP as istu,
         |                     MEGP as isme,
         |                     ENGP as isen,
         |                     CBGP as iscb,
         |                     BGROUP as isob,
         |                     stepno2,
         |                     stepno3,
         |                     rank3,
         |                     stepno4,
         |                     stepno5,
         |                     stepno6,
         |                     stepno7,
         |                     rank7,
         |                     stepno8,
         |                     rank8,
         |                     stepno9,
         |                     rank9,
         |                     max(xrunduration) over (partition by basearea, BGROUP, TEGP) as runduration,
         |                     min(xeig_starttime) over(partition by basearea, BGROUP, TEGP) as eig_starttime,
         |                     max(xeig_duration) over(partition by basearea, BGROUP, TEGP) as eig_duration,
         |                     max(xeig_initial_cposition) over(partition by basearea, BGROUP, TEGP) as eig_initial_cposition,
         |                     max(xeig_end_cposition) over(partition by basearea, BGROUP, TEGP) as eig_end_cposition,
         |                     max(xeig_min_mheater) over(partition by basearea, BGROUP, TEGP) as eig_min_mheater,
         |                     max(xeig_initial_sdiameter) over(partition by basearea, BGROUP, TEGP) as eig_initial_setdiameter,
         |                     max(xeig_max_mheater) over(partition by basearea, BGROUP, TEGP) as eig_max_mheater,
         |                     max(xeig_avg_bheater) over(partition by basearea, BGROUP, TEGP) as eig_avg_bheater,
         |                     max(xeig_min_meltlevel) over(partition by basearea, BGROUP, TEGP) as eig_min_meltlevel,
         |                     max(xeig_end_meltlevel) over(partition by basearea, BGROUP, TEGP) as eig_end_meltlevel,
         |                     max(xeig_end_crystallength) over(partition by basearea, BGROUP, TEGP) as eig_end_crystallength,
         |                     max(xeig_avg_mpressure) over(partition by basearea, BGROUP, TEGP) as eig_avg_mpressure,
         |                     max(xeig_avg_argonflow) over(partition by basearea, BGROUP, TEGP) as eig_avg_argonflow,
         |                     max(xeig_avg_pumpfrequency) over(partition by basearea, BGROUP, TEGP) as eig_avg_pumpfrequency,
         |                     max(xeig_end_reweight) over(partition by basearea, BGROUP, TEGP) as eig_end_reweight,
         |                     max(xeig_front300_avgcrotation) over(PARTITION BY basearea, bgroup, TEGP) as eig_front300_avgcrotation,   -- 等径前300mm埚转均值
         |                     max(xeig_rear30_avgcrotation) over(PARTITION BY basearea, bgroup, TEGP) as eig_rear30_avgcrotation,       -- 等径后30min埚转均值
         |                     max(xeig_front30_avgsrotation) over(PARTITION BY basearea, bgroup, TEGP) as eig_front30_avgsrotation,       -- 等径后30min晶转均值
         |                     max(xeig_rear30_avgsrotation) over(PARTITION BY basearea, bgroup, TEGP) as eig_rear30_avgsrotation,       -- 等径后30min晶转均值
         |                     max(xeig_front5_avgdiameter) over(PARTITION BY basearea, bgroup, TEGP) as eig_front5_avgdiameter,       -- 等径前5mm直径均值
         |                     max(xeig_local100_avgclift) over(PARTITION BY basearea, bgroup, TEGP) as eig_local100_avgclift,         -- 等径0-100mm埚升均值
         |                     max(xeig_local200_avgclift) over(PARTITION BY basearea, bgroup, TEGP) as eig_local200_avgclift,         -- 等径100-200mm埚升均值
         |                     max(xeig_local300_avgclift) over(PARTITION BY basearea, bgroup, TEGP) as eig_local300_avgclift,         -- 等径200-300mm埚升均值
         |                     max(xeig_local400_avgclift) over(PARTITION BY basearea, bgroup, TEGP) as eig_local400_avgclift,         -- 等径300-400mm埚升均值
         |                     max(xeig_local500_avgclift) over(PARTITION BY basearea, bgroup, TEGP) as eig_local500_avgclift,         -- 等径400-500mm埚升均值
         |                     max(xeig_front150_maxdiff_diameter) over(partition by basearea, BGROUP, TEGP) as eig_front150_maxdiff_diameter,
         |                     max(xeig_front150_mindiff_diameter) over(partition by basearea, BGROUP, TEGP) as eig_front150_mindiff_diameter,
         |                     max(xeig_front500_avgspeed) over(partition by basearea, BGROUP, TEGP) as eig_front500_avgspeed,
         |                     max(xeig_mid1200_avgspeed) over(partition by basearea, BGROUP, TEGP) as eig_mid1200_avgspeed,
         |                     max(xeig_front500_maxclift) over(partition by basearea, BGROUP, TEGP) as eig_front500_maxclift,
         |                     max(xeig_front500_maxclift_duration) over(partition by basearea, BGROUP, TEGP) as eig_front500_maxclift_duration,
         |                     -- ! 新增 ！--
         |                     max(xeig_avgsmeltlevel) over(partition by basearea, BGROUP, TEGP) as eig_avgsmeltlevel,
         |                     max(xeig_entirety_avgspeed) over(partition by basearea, BGROUP, TEGP) as eig_entirety_avgspeed,
         |                     max(xeig_local500_avgmeltlevel) over(partition by basearea, BGROUP, TEGP) as eig_local500_avgmeltlevel,
         |                     max(xeig_local1000_avgmeltlevel) over(partition by basearea, BGROUP, TEGP) as eig_local1000_avgmeltlevel,
         |                     max(xeig_local1500_avgmeltlevel) over(partition by basearea, BGROUP, TEGP) as eig_local1500_avgmeltlevel,
         |                     min(xeig_meltlevellt1_clength) over(partition by basearea, BGROUP, TEGP) as eig_meltlevellt1_clength,
         |                     max(xeig_local100_avgspeed) over(partition by basearea, BGROUP, TEGP) as eig_local100_avgspeed,
         |                     max(xeig_local200_avgspeed) over(partition by basearea, BGROUP, TEGP) as eig_local200_avgspeed,
         |                     max(xeig_local300_avgspeed) over(partition by basearea, BGROUP, TEGP) as eig_local300_avgspeed,
         |                     max(xeig_local400_avgspeed) over(partition by basearea, BGROUP, TEGP) as eig_local400_avgspeed,
         |                     max(xeig_local500_avgspeed) over(partition by basearea, BGROUP, TEGP) as eig_local500_avgspeed,
         |                     max(xeig_local1200_avgspeed) over(partition by basearea, BGROUP, TEGP) as eig_local1200_avgspeed,
         |                     max(xeig_local1500_avgsetspeed) over(partition by basearea, BGROUP, TEGP) as eig_local1500_avgsetspeed,
         |                     max(xeig_local100_stdspeed) over(partition by basearea, BGROUP, TEGP) as eig_local100_stdspeed,
         |                     max(xeig_local200_stdspeed) over(partition by basearea, BGROUP, TEGP) as eig_local200_stdspeed,
         |                     max(xeig_local300_stdspeed) over(partition by basearea, BGROUP, TEGP) as eig_local300_stdspeed,
         |                     max(xeig_local400_stdspeed) over(partition by basearea, BGROUP, TEGP) as eig_local400_stdspeed,
         |                     max(xeig_local500_stdspeed) over(partition by basearea, BGROUP, TEGP) as eig_local500_stdspeed,
         |                     max(xeig_local1200_stdspeed) over(partition by basearea, BGROUP, TEGP) as eig_local1200_stdspeed,
         |                     max(xeig_local100_stddiffspeed) over(partition by basearea, BGROUP, TEGP) as eig_local100_stddiffspeed,
         |                     max(xeig_local200_stddiffspeed) over(partition by basearea, BGROUP, TEGP) as eig_local200_stddiffspeed,
         |                     max(xeig_local300_stddiffspeed) over(partition by basearea, BGROUP, TEGP) as eig_local300_stddiffspeed,
         |                     max(xeig_local400_stddiffspeed) over(partition by basearea, BGROUP, TEGP) as eig_local400_stddiffspeed,
         |                     max(xeig_local500_stddiffspeed) over(partition by basearea, BGROUP, TEGP) as eig_local500_stddiffspeed,
         |                     max(xeig_local1200_stddiffspeed) over(partition by basearea, BGROUP, TEGP) as eig_local1200_stddiffspeed,
         |                     max(xeig_local100_stddiameter) over(partition by basearea, BGROUP, TEGP) as eig_local100_stddiameter,
         |                     max(xeig_local200_stddiameter) over(partition by basearea, BGROUP, TEGP) as eig_local200_stddiameter,
         |                     max(xeig_local300_stddiameter) over(partition by basearea, BGROUP, TEGP) as eig_local300_stddiameter,
         |                     max(xeig_local400_stddiameter) over(partition by basearea, BGROUP, TEGP) as eig_local400_stddiameter,
         |                     max(xeig_local500_stddiameter) over(partition by basearea, BGROUP, TEGP) as eig_local500_stddiameter,
         |                     max(xeig_local1200_stddiameter) over(partition by basearea, BGROUP, TEGP) as eig_local1200_stddiameter,
         |                     max(xeig_local100_stddiffdiameter) over(partition by basearea, BGROUP, TEGP) as eig_local100_stddiffdiameter,
         |                     max(xeig_local200_stddiffdiameter) over(partition by basearea, BGROUP, TEGP) as eig_local200_stddiffdiameter,
         |                     max(xeig_local300_stddiffdiameter) over(partition by basearea, BGROUP, TEGP) as eig_local300_stddiffdiameter,
         |                     max(xeig_local400_stddiffdiameter) over(partition by basearea, BGROUP, TEGP) as eig_local400_stddiffdiameter,
         |                     max(xeig_local500_stddiffdiameter) over(partition by basearea, BGROUP, TEGP) as eig_local500_stddiffdiameter,
         |                     max(xeig_local1200_stddiffdiameter) over(partition by basearea, BGROUP, TEGP) as eig_local1200_stddiffdiameter,
         |                     min(xnin_starttime) over(partition by basearea, BGROUP, TEGP) as nin_starttime,
         |                     max(xnin_duration) over(partition by basearea, BGROUP, TEGP) as nin_duration,
         |                     max(xnin_end_reweight) over(partition by basearea, BGROUP, TEGP) as nin_end_reweight,
         |                     max(xnin_tail_weight) over(partition by basearea, BGROUP, TEGP) as nin_tail_weight,
         |                     max(xnin_tail_length) over(partition by basearea, BGROUP, TEGP) as nin_tail_length,
         |                     -- ！化料阶段四次组合指标！--
         |                     max(fishead) over(partition by basearea, BGROUP) as ishead,        --- 是否首段
         |                     min(xtwo_starttime) over(partition by basearea, BGROUP) as two_starttime,
         |                     max(xtwo_duration) over(partition by basearea, BGROUP) as two_duration,
         |                     max(xtwo_initial_reweight) over(partition by basearea, BGROUP) as two_initial_reweight,
         |                     max(xtwo_end_reweight) over(partition by basearea, BGROUP) as two_end_reweight,
         |                     max(xtwo_avg_mpressure) over(partition by basearea, BGROUP) as two_avg_mpressure,
         |                     max(xtwo_min_mpressure) over(partition by basearea, BGROUP) as two_min_mpressure,
         |                     max(xtwo_max_mpressure) over(partition by basearea, BGROUP) as two_max_mpressure,
         |                     max(xtwo_std_mpressure) over(partition by basearea, BGROUP) as two_std_mpressure,
         |                     max(xtwo_avg_mheater) over(partition by basearea, BGROUP) as two_avg_mheater,
         |                     max(xtwo_avg_bheater) over(partition by basearea, BGROUP) as two_avg_bheater,
         |                     max(xtwo_end_cposition) over(partition by basearea, BGROUP) as two_end_cposition,
         |                     max(xtwo_avg5_crotation) over(partition by basearea, BGROUP) as two_avg5_crotation,
         |                     max(xtwo_mheater_energy) over(partition by basearea, BGROUP) as two_mheater_energy,
         |                     max(xtwo_bheater_energy) over(partition by basearea, BGROUP) as two_bheater_energy,
         |                     max(xtwo_avg_mresistance) over(partition by basearea, BGROUP) as two_avg_mresistance,
         |                     max(xtwo_avg_bresistance) over(partition by basearea, BGROUP) as two_avg_bresistance,
         |                     max(xtwo_feed_quanitity) over(partition by basearea, BGROUP) as two_feed_quanitity,
         |                     max(xtwo_feed_efficiency) over(partition by basearea, BGROUP) as two_feed_efficiency,
         |                     max(xtwo_preheater_duration) over(partition by basearea, BGROUP) as two_preheater_duration,
         |                     max(xtwo_last_meltduration) over(partition by basearea, BGROUP) as two_last_meltduration,
         |                     min(ttwo_min_cposition) over(PARTITION BY basearea, bgroup) as two_start_cposition,  -- 化料初始埚位
         |                     --*2022-07-22（新增）*--
         |                     max(xtwo_max_setmheater) over(PARTITION BY basearea, bgroup) as two_max_setmheater,
         |                     max(xtwo_max_setbheater) over(PARTITION BY basearea, bgroup) as two_max_setbheater,
         |                     max(xtwo_avg_argonflow) over(PARTITION BY basearea, bgroup) as two_avg_argonflow,
         |                     min(xthr_starttime) over(partition by basearea, BGROUP, CBGP) as thr_starttime,
         |                     max(xthr_duration) over(partition by basearea, BGROUP, CBGP) as thr_duration,
         |                     max(xthr_end_reweight) over(partition by basearea, BGROUP, CBGP) as thr_end_reweight,
         |                     max(xthr_end_cposition) over(partition by basearea, BGROUP, CBGP) as thr_end_cposition,
         |                     max(xthr_max_diffsurftemp) over(partition by basearea, BGROUP, CBGP) as thr_max_diffsurftemp,
         |                     max(xthr_min_diffsurftemp) over(partition by basearea, BGROUP, CBGP) as thr_min_diffsurftemp,
         |                     max(xthr_std_diffsurftemp) over(partition by basearea, BGROUP, CBGP) as thr_std_diffsurftemp,
         |                     max(xthr_avg5_crotation) over(partition by basearea, BGROUP, CBGP) as thr_avg5_crotation,
         |                     max(xthr_avg5_mpressure) over(partition by basearea, BGROUP, CBGP) as thr_avg5_mpressure,
         |                     max(xthr_avg5_meltlevel) over(partition by basearea, BGROUP, CBGP) as thr_avg5_meltlevel,
         |                     max(xthr_mheater_energy) over(partition by basearea, BGROUP, CBGP) as thr_mheater_energy,
         |                     max(xthr_bheater_energy) over(partition by basearea, BGROUP, CBGP) as thr_bheater_energy,
         |                     max(xthr_sum_areasurftemp) over(partition by basearea, BGROUP, CBGP) as thr_sum_areasurftemp,
         |                     min(xfou_starttime) over(partition by basearea, BGROUP, TEGP) as fou_starttime,
         |                     max(xfou_duration) over(partition by basearea, BGROUP, TEGP) as fou_duration,
         |                     max(xfou_surftemp_area) over(partition by basearea, BGROUP, TEGP) as fou_surftemp_area,
         |                     max(xfou_min_smheater) over(partition by basearea, BGROUP, TEGP) as fou_min_smheater,
         |                     max(xfou_initial_mheater) over(partition by basearea, BGROUP, TEGP) as fou_initial_mheater,
         |                     max(xfou_avg_bheater) over(partition by basearea, BGROUP, TEGP) as fou_avg_bheater,
         |                     max(xfou_avg_meltlevel) over(partition by basearea, BGROUP, TEGP) as fou_avg_meltlevel,
         |                     max(xfou_avg_crotation) over(partition by basearea, BGROUP, TEGP) as fou_avg_crotation,
         |                     max(xfou_avg_cposition) over(partition by basearea, BGROUP, TEGP) as fou_avg_cposition,
         |                     max(xfou_avg_mpressure) over(partition by basearea, BGROUP, TEGP) as fou_avg_mpressure,
         |                     max(xfou_avg_argonflow) over(partition by basearea, BGROUP, TEGP) as fou_avg_argonflow,
         |                     max(xfou_min_crystalpos) over(partition by basearea, BGROUP, TEGP) as fou_min_crystalpos,
         |                     min(xfou_start_heatexchangerpos) over(partition by basearea, BGROUP, TEGP) as fou_start_heatexchangerpos,
         |                     max(xfou_last_setmheater) over(partition by basearea, BGROUP, TEGP) as fou_last_setmheater,
         |                     max(xfou_avg_diffsurftemp) over(partition by basearea, BGROUP, TEGP) as fou_avg_diffsurftemp,
         |                     max(xfou_min_diffsurftemp) over(partition by basearea, BGROUP, TEGP) as fou_min_diffsurftemp,
         |                     max(xfou_max_diffsurftemp) over(partition by basearea, BGROUP, TEGP) as fou_max_diffsurftemp,
         |                     max(xfou_std_diffsurftemp) over(partition by basearea, BGROUP, TEGP) as fou_std_diffsurftemp,
         |                     min(xfou_initial_diffsurftemp) over(partition by basearea, BGROUP, TEGP) as fou_initial_diffsurftemp,
         |                     max(xfou_sum_areasurftemp) over(partition by basearea, BGROUP, TEGP) as fou_sum_areasurftemp,
         |                     max(xfou_minsmheater_duration) over(partition by basearea, BGROUP, TEGP) as fou_minsmheater_duration,
         |                     max(xfou_steady_duration) over(partition by basearea, BGROUP, TEGP) as fou_steady_duration,
         |                     max(xfou_weld2_diameter) over(partition by basearea, BGROUP, TEGP) as fou_weld2_diameter,
         |                     max(xfou_weld2_setmeltsurftemp) over(partition by basearea, BGROUP, TEGP) as fou_weld2_setmeltsurftemp,
         |                     max(xfou_weld2_meltsurftemp) over(partition by basearea, BGROUP, TEGP) as fou_weld2_meltsurftemp,
         |                     max(xfou_weld2_avgdiameter) over(partition by basearea, bgroup) as fou_weld2_avgdiameter, -- 调温熔接2min后直径均值
         |                     max(xfou_weld_initial_diameter) over(partition by basearea, BGROUP, TEGP) as fou_weld_initial_diameter,     --新增熔接起始直径
         |                     --*2022-07-22(新增)*--
         |                     max(xfou_steadyfirst_diameter) over(partition by basearea, BGROUP, TEGP) as fou_steadyfirst_diameter,
         |                     min(xfiv_starttime) over(partition by basearea, BGROUP, TEGP) as fiv_starttime,
         |                     max(xfiv_duration) over(partition by basearea, BGROUP, TEGP) as fiv_duration,
         |                     max(xfiv_avg_mheater) over(partition by basearea, BGROUP, TEGP) as fiv_avg_mheater,
         |                     max(xfiv_avg_bheater) over(partition by basearea, BGROUP, TEGP) as fiv_avg_bheater,
         |                     max(xfiv_avg_meltlevel) over(partition by basearea, BGROUP, TEGP) as fiv_avg_meltlevel,
         |                     max(xfiv_avg_crotation) over(partition by basearea, BGROUP, TEGP) as fiv_avg_crotation,
         |                     max(xfiv_avg_cposition) over(partition by basearea, BGROUP, TEGP) as fiv_avg_cposition,
         |                     max(xfiv_avg_mpressure) over(partition by basearea, BGROUP, TEGP) as fiv_avg_mpressure,
         |                     max(xfiv_avg_argonflow) over(partition by basearea, BGROUP, TEGP) as fiv_avg_argonflow,
         |                     max(xfiv_avg_pumpfrequency) over(partition by basearea, BGROUP, TEGP) as fiv_avg_pumpfrequency,
         |                     max(xfiv_avg_srotation) over(partition by basearea, BGROUP, TEGP) as fiv_avg_srotation,
         |                     max(xfiv_end_crystallength) over(partition by basearea, BGROUP, TEGP) as fiv_end_crystallength,
         |                     max(xfiv_sum_areadiffsurftemp) over(PARTITION BY basearea, bgroup, TEGP) as fiv_sum_areadiffsurftemp, -- 引晶液温差面积
         |                     max(xfiv_extre_diffsurftemp) over(partition by basearea, BGROUP, TEGP) as fiv_extre_diffsurftemp,
         |                     max(xfiv_avg_diffsurftemp) over(partition by basearea, BGROUP, TEGP) as fiv_avg_diffsurftemp,
         |                     max(xfiv_std_diffsurftemp) over(partition by basearea, BGROUP, TEGP) as fiv_std_diffsurftemp,
         |                     max(xfiv_avg_speed1) over(PARTITION BY basearea, bgroup, TEGP) as fiv_avg_speed1,        -- 引晶100mm后拉速均值
         |                     max(xfiv_avg_speed2) over(PARTITION BY basearea, bgroup, TEGP) as fiv_avg_speed2,        -- 引晶130mm后拉速均值
         |                     max(xfiv_avg_speed3) over(PARTITION BY basearea, bgroup, TEGP) as fiv_avg_speed3,        -- 引晶160mm后拉速均值
         |                     max(xfiv_entirety_speed) over(PARTITION BY basearea, bgroup, TEGP) as fiv_entirety_speed,   -- 引晶全程平均拉速
         |                     max(xfiv_rear100_avgdiameter) over(PARTITION BY basearea, bgroup, TEGP) as fiv_rear100_avgdiameter, -- 引晶后100mm直径均值
         |                     max(xfiv_rear100_mindiameter) over(PARTITION BY basearea, bgroup, TEGP) as fiv_rear100_mindiameter, -- 引晶后100mm直径最小值
         |                     max(xfiv_rear100_maxdiameter) over(PARTITION BY basearea, bgroup, TEGP) as fiv_rear100_maxdiameter, -- 引晶后100mm直径最大值
         |                     max(xfiv_rear100_stddiameter) over(PARTITION BY basearea, bgroup, TEGP) as fiv_rear100_stddiameter, -- 引晶后100mm直径标准差
         |                     max(xfiv_diameter0) over(PARTITION BY basearea, bgroup, TEGP) as fiv_start_diameter,  -- 引晶起始直径(新增)
         |                     max(xfiv_diameterB) over(PARTITION BY basearea, bgroup, TEGP) as fiv_end_diameter,  -- 引晶终止直径(新增)
         |                     max(xfiv_diameter1) over(partition by basearea, BGROUP, TEGP) as fiv_diameter1,     -- 新增
         |                     max(xfiv_diameter2) over(partition by basearea, BGROUP, TEGP) as fiv_diameter2,     -- 新增
         |                     max(xfiv_diameter3) over(partition by basearea, BGROUP, TEGP) as fiv_diameter3,     -- 新增
         |                     max(xfiv_diameter4) over(partition by basearea, BGROUP, TEGP) as fiv_diameter4,     -- 新增
         |                     max(xfiv_diameter5) over(partition by basearea, BGROUP, TEGP) as fiv_diameter5,     -- 新增
         |                     max(xfiv_diameter6) over(partition by basearea, BGROUP, TEGP) as fiv_diameter6,     -- 新增
         |                     max(xfiv_diameter7) over(partition by basearea, BGROUP, TEGP) as fiv_diameter7,     -- 新增
         |                     max(xfiv_diameter8) over(partition by basearea, BGROUP, TEGP) as fiv_diameter8,     -- 新增
         |                     max(xfiv_diameter9) over(partition by basearea, BGROUP, TEGP) as fiv_diameter9,     -- 新增
         |                     max(xfiv_diameterA) over(partition by basearea, BGROUP, TEGP) as fiv_diameterA,     -- 新增
         |                     --*2022-07-22(新增)*--
         |                     max(xfiv_diameterlt6_clength) over(partition by basearea, BGROUP, TEGP) as fiv_diameterlt6_clength,
         |                     min(xsix_starttime) over(partition by basearea, BGROUP, TEGP) as six_starttime,
         |                     max(xsix_duration) over(partition by basearea, BGROUP, TEGP) as six_duration,
         |                     max(xsix_avg_mheater) over(partition by basearea, BGROUP, TEGP) as six_avg_mheater,
         |                     max(xsix_avg_bheater) over(partition by basearea, BGROUP, TEGP) as six_avg_bheater,
         |                     max(xsix_avg_meltlevel) over(partition by basearea, BGROUP, TEGP) as six_avg_meltlevel,
         |                     max(xsix_avg_crotation) over(partition by basearea, BGROUP, TEGP) as six_avg_crotation,
         |                     max(xsix_avg_cposition) over(partition by basearea, BGROUP, TEGP) as six_avg_cposition,
         |                     max(xsix_avg_mpressure) over(partition by basearea, BGROUP, TEGP) as six_avg_mpressure,
         |                     max(xsix_avg_argonflow) over(partition by basearea, BGROUP, TEGP) as six_avg_argonflow,
         |                     max(xsix_avg_speed) over(partition by basearea, BGROUP, TEGP) as six_avg_speed,
         |                     max(xsix_avg_pumpfrequency) over(partition by basearea, BGROUP, TEGP) as six_avg_pumpfrequency,
         |                     max(xsix_avg_srotation) over(partition by basearea, BGROUP, TEGP) as six_avg_srotation,
         |                     max(xsix_end_crystallength) over(partition by basearea, BGROUP, TEGP) as six_end_crystallength,
         |                     max(xsix_end_diameter) over(partition by basearea, BGROUP, TEGP) as six_end_diameter,
         |                     max(xsix_initial_diameter) over(partition by basearea, BGROUP, TEGP) as six_initial_diameter,
         |                     max(xsix_initial_meltlevel) over(partition by basearea, BGROUP, TEGP) as six_initial_meltlevel,
         |                     max(xsix_smheater_decay) over(partition by basearea, BGROUP,TEGP) as six_smheater_decay,
         |                     max(fsix_diameter1) over(PARTITION BY basearea, bgroup, TEGP) as six_diameter1,
         |                     max(fsix_diameter2) over(PARTITION BY basearea, bgroup, TEGP) as six_diameter2,
         |                     max(fsix_diameter3) over(PARTITION BY basearea, bgroup, TEGP) as six_diameter3,
         |                     max(fsix_diameter4) over(PARTITION BY basearea, bgroup, TEGP) as six_diameter4,
         |                     max(fsix_diameter5) over(PARTITION BY basearea, bgroup, TEGP) as six_diameter5,
         |                     max(fsix_diameter6) over(PARTITION BY basearea, bgroup, TEGP) as six_diameter6,
         |                     max(fsix_diameter7) over(PARTITION BY basearea, bgroup, TEGP) as six_diameter7,
         |                     max(fsix_diameter8) over(PARTITION BY basearea, bgroup, TEGP) as six_diameter8,
         |                     max(fsix_diameter9) over(PARTITION BY basearea, bgroup, TEGP) as six_diameter9,
         |                     max(fsix_diameterA) over(PARTITION BY basearea, bgroup, TEGP) as six_diameterA,
         |                     min(xsev_starttime) over(partition by basearea, BGROUP, TEGP) as sev_starttime,
         |                     min(xsev_initial_diameter) over(partition by basearea, BGROUP, TEGP) as sev_initial_diameter,
         |                     max(xsev_duration) over(partition by basearea, BGROUP, TEGP) as sev_duration,
         |                     max(xsev_max_avgslspeed) over(partition by basearea, BGROUP, TEGP) as sev_max_avgslspeed,
         |                     max(xsev_diff_setmheater) over(partition by basearea, BGROUP, TEGP) as sev_diff_setmheater,
         |                     -- add 2022-11-08
         |                     max(ttwo_init_sbheater) over(PARTITION BY basearea, bgroup) as two_init_sbheater,
         |                     max(ttwo_init_smheater) over(PARTITION BY basearea, bgroup) as two_init_smheater,
         |                     max(ttwo_init_bheater_diff) over(PARTITION BY basearea, bgroup) as two_init_bheater_diff,
         |                     max(ttwo_init_mheater_diff) over(PARTITION BY basearea, bgroup) as two_init_mheater_diff,
         |                     max(tsix_init_temp_diff) over(PARTITION BY basearea, bgroup, TEGP) as six_init_temp_diff,
         |                     max(tsix_100_temp_diff) over(PARTITION BY basearea, bgroup, TEGP) as six_100_temp_diff,
         |                     max(xthr_crotation_gt5_smheater_drop_duration) over(partition by basearea, BGROUP, CBGP) as thr_crotation_gt5_smheater_drop_duration,
         |                     max(xthr_cbot_to_crotation_gt5_duration) over(partition by basearea, BGROUP, CBGP) as thr_cbot_to_crotation_gt5_duration,
         |                     max(xthr_smheater_drop_to_temp_duration) over(partition by basearea, BGROUP, CBGP) as thr_smheater_drop_to_temp_duration,
         |                     max(teig_front500_clift_map_clength) over(partition by basearea, BGROUP, TEGP) as eig_front500_clift_map_clength,
         |                     row_number() over(PARTITION BY basearea, bgroup, TEGP order by nowtime desc) as trank,
         |                     -- add 2023-04-24 增加等径主加功率
         |                     max(eig_local100_mheater) over(partition by basearea, BGROUP, TEGP) as eig_end100_mheater,
         |                     max(eig_local200_mheater) over(partition by basearea, BGROUP, TEGP) as eig_end200_mheater,
         |                     max(eig_local300_mheater) over(partition by basearea, BGROUP, TEGP) as eig_end300_mheater,
         |                     max(eig_local400_mheater) over(partition by basearea, BGROUP, TEGP) as eig_end400_mheater,
         |                     max(eig_local500_mheater) over(partition by basearea, BGROUP, TEGP) as eig_end500_mheater,
         |                     max(eig_local1000_mheater) over(partition by basearea, BGROUP, TEGP) as eig_end1000_mheater,
         |                     max(eig_local1500_mheater) over(partition by basearea, BGROUP, TEGP) as eig_end1500_mheater,
         |                     max(eig_local2000_mheater) over(partition by basearea, BGROUP, TEGP) as eig_end2000_mheater,
         |                     max(eig_local2500_mheater) over(partition by basearea, BGROUP, TEGP) as eig_end2500_mheater,
         |                     max(eig_local3000_mheater) over(partition by basearea, BGROUP, TEGP) as eig_end3000_mheater,
         |                     max(eig_local3500_mheater) over(partition by basearea, BGROUP, TEGP) as eig_end3500_mheater
         |
         |            from sixth_layer_table
         |              where (stepno4>0 and rank4=1)
         |                 or (stepno2>0)
         |                 or (stepno3>0)
         |                 or (stepno5>0)
         |                 or (stepno6>0)
         |                 or (stepno7>0)
         |                 or (stepno8>0)
         |                 or (stepno9>0)
         | ) sevth_layer_table
         | where trank=1 and iste > 0 and two_starttime is not null
    """.stripMargin)
    //    kylin_table.coalesce(10).createOrReplaceTempView("kylin_table")
    kylin_table.repartition(10).createOrReplaceTempView("kylin_table")
    //    kylin_table.show(10)
    println("-----------test9----------")
    spark.sql(
      s"""
         |insert overwrite table dwd_pp_ingot.ingot_device_portrait partition(day_id)
         |select * from kylin_table
      """.stripMargin)
    println("hive写入成功......")
    spark.stop()
  }
}
