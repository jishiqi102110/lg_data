package warehouse.dwd.scada.djdata

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, substring}
import util.SparkUtil

/*
此代码为基地分析单晶数据模块
	su - liangwt -c "spark-submit --class warehouse.dwd.scada.djdata.JiDiDataAnalyse --master yarn --deploy-mode client --driver-memory 10g --num-executors 10 --executor-memory 8g --executor-cores 4 --queue spark --conf spark.shuffle.service.enabled=true --conf spark.sql.session.timeZone=Asia/Shanghai --conf spark.sql.catalogImplementation=hive --name JiDiDataAnalyse --jars /home/liangwt/jar/hutool-core-5.8.18.jar,/home/liangwt/jar/hutool-json-5.8.18.jar /home/liangwt/jar/silicon_data_flow-1.0-SNAPSHOT-dependencies.jar"

 */
object JiDiDataAnalyse {
  val spark = SparkUtil.hiveSparkSession("JiDiDataAnalyse")

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    var stove = ""
    if (args.size > 0) {
      stove = args(0)
      println("stove:" + stove)
    }
    //todo 改写入方式

    // step1 基础rzc分组
    rzcGroup(stove)
    // step2 表一计算 二次加料氩气流量
    //agg_script_ar_flow
    // step3 表二计算 籽晶预热以及熔接位置
    agg_script_merged_feature
    // step4 表三计算
    //agg_temp_slope
    // step5 表四计算
    //agg_lower_he
    // step6 表五计算
    //agg_cp_detail

    spark.stop()

  }


  // rcz分组
  def rzcGroup(stove: String): Unit = {
    val ingot_result_sql =
      s"""
         |    SELECT
         |        t.*,
         |        LAG(t.setbottomheater, 1) OVER(PARTITION BY t.basearea, t.crystalid ORDER BY t.nowtime) AS last_sbheater,
         |        LAG(t.setmainheater, 1) OVER(PARTITION BY t.basearea, t.crystalid ORDER BY t.nowtime) AS last_smheater,
         |        CASE WHEN ((t.state >= 4) AND (t.state < 9)) THEN state ELSE 0 END AS fill_state
         |    FROM ods_ingot_scada.djzk_app_result t
         |    -- WHERE t.BASEAREA IN ('QJ_G50');
         |    WHERE left(basearea,2)='QJ'
         |    and left (SUBSTR(BASEAREA, -3),1) in ('A','B','C','D')
         |    and  day_id >= '2023-01-01' and  day_id <= '2023-09-20'
         |""".stripMargin

    val ingot_result_sql_DF = spark.sql(ingot_result_sql)
    ingot_result_sql_DF.createTempView("ingot_result")


    val ods_table_sql =
      """
        |SELECT t1.*,
        |           SUM(t1.XISOPENBOT) OVER(PARTITION BY t1.basearea, t1.CRYSTALID ORDER BY t1.NOWTIME) AS XBGROUP
        |    FROM (
        |      SELECT t0.*,
        |             SUBSTR(t0.BASEAREA, -3) AS puller,
        |             CASE WHEN ((t0.last_sbheater + t0.last_smheater < 130) AND (t0.setbottomheater + t0.setmainheater >= 130)) THEN 1 ELSE 0 END AS xisopenbot
        |      FROM ingot_result t0
        |    ) t1
        |""".stripMargin

    val ods_table_sql_DF = spark.sql(ods_table_sql)
    ods_table_sql_DF.createTempView("ods_table")


    val rcz_table_sql =
      """
        |             select t11.* from (SELECT *,
        |                                       SUM(CASE
        |                                               WHEN MAXSTATE  IN (4, 5, 6, 7, 8, 9) AND LAG_MAXSTATE =0 and  LAG2_MAXSTATE IN (4, 5, 6, 7, 8, 9)
        |                                                THEN 0 ELSE xisopenbot END)
        |                                           OVER (PARTITION BY basearea, CRYSTALID ORDER BY nowtime) AS BGROUP
        |                                FROM
        |                                     (SELECT *,
        |                                              LAG(XLAG_MAXSTATE, 1) OVER(PARTITION BY basearea, CRYSTALID ORDER BY nowtime) AS LAG2_MAXSTATE
        |                                      from
        |                                           (SELECT *,
        |                                                MAX(LAG_MAXSTATE) OVER (PARTITION BY basearea, CRYSTALID, XBGROUP) AS XLAG_MAXSTATE
        |                                           FROM (SELECT *,
        |                                              LAG(MAXSTATE, 1) OVER(PARTITION BY t0.basearea, t0.CRYSTALID ORDER BY t0.nowtime) AS LAG_MAXSTATE
        |                                             FROM (
        |                                                SELECT *,
        |                                                       MAX(fill_state) OVER (PARTITION BY basearea, CRYSTALID, XBGROUP) AS MAXSTATE
        |                                                FROM ods_table
        |                                                 ) t0
        |                                            ) t1
        |                                         )t
        |                                     )A
        |
        |                 )t11 where t11.BGROUP>0
        |""".stripMargin

    val rcz_table_sql_DF = spark.sql(rcz_table_sql).drop("LAG2_MAXSTATE","XLAG_MAXSTATE","LAG_MAXSTATE")
    //todo 最后注释掉
    println("rcz分组 完成")
    //rcz_table_sql_DF.show(10)
    rcz_table_sql_DF.createTempView("rcz_table")
  }

  // 二次加料氩气流量
  def agg_script_ar_flow(): Unit = {
    val ar_flow_sql =
      s"""
         |SELECT c.*
         |FROM (SELECT b.*,
         |             row_number() over (partition by BASEAREA,CRYSTALID,BGROUP,feedgroup order by CRYSTALPOS) as rk
         |      FROM (SELECT
         |                sum(a.openclose) over (partition by BASEAREA,CRYSTALID,BGROUP order by NOWTIME) as feedgroup,
         |                a.*
         |            FROM (SELECT a.*,
         |                         case
         |                             when (CRYSTALWEIGHT < 35 and
         |                                   lag(CRYSTALWEIGHT, 1) over (partition by BASEAREA,CRYSTALID,BGROUP order by NOWTIME) >=35)
         |                                 then 1
         |                             else 0 end openclose
         |                  FROM RCZ_TABLE a
         |                  WHERE STATE = 23) a) b
         |      WHERE b.SUBPRESSURE1000T < 30) c
         |WHERE c.rk = 1
         |""".stripMargin
    val ar_flow_sql_DF = spark.sql(ar_flow_sql).withColumn("day", substring(col("nowtime"), 1, 11))

    //todo 最后正式上线前去掉
    //ar_flow_sql_DF.show(10)
    ar_flow_sql_DF.createTempView("ar_flow_view")
    //ar_flow_sql_DF.printSchema()
    // todo 这里最后入库

    val ar_flow_view_sql =
      """
        |INSERT OVERWRITE TABLE test.agg_script_ar_flow_prd partition(day)
        |SELECT * FROM ar_flow_view
        |""".stripMargin
    spark.sql(ar_flow_view_sql)

    println("二次加料氩气流量 写入成功")
  }


  //籽晶预热以及熔接位置  打锅转 放埚位 双目标 合并特征表
  def agg_script_merged_feature(): Unit = {
    // 籽晶预热以及熔接位置
    val melt_phase_sql =
      s"""
         |SELECT a.BASEAREA,
         |           a.CRYSTALId,
         |           a.cspos_group,
         |           a.BGROUP,
         |           sum(a.timediff) AS DURATION,
         |           max(MELTSURFTEMP) AS MAX_MELTSURFTEMP,
         |           --APPX_MEDIAN(a.CRYSTALPOS) AS PHASE_POS
         |           --percentile_cont(0.5) WITHIN GROUP (ORDER BY your_column) AS median_value
         |           percentile(a.CRYSTALPOS, 0.5) AS PHASE_POS
         |        FROM (
         |        SELECT
         |             (unix_timestamp(NOW()) - unix_timestamp(lag(NOW()) over(partition by BASEAREA,CRYSTALID,BGROUP order by NOW())))*24*60 as timediff,
         |               CRYSTALPOS- lag(CRUCIBLEPOS,1) over(partition by BASEAREA,CRYSTALID,BGROUP order by NOW()) as posdiff,
         |               case
         |                   when CRYSTALPOS>0 and CRYSTALPOS<=200 then '0-200'
         |                   when CRYSTALPOS>200 and CRYSTALPOS<=400 then '200-400'
         |                   when CRYSTALPOS>400 and CRYSTALPOS<=600 then '400-600'
         |                   when CRYSTALPOS>600 and CRYSTALPOS<=800 then '600-800'
         |                   when CRYSTALPOS>800 then 'others'
         |               end cspos_group,
         |               a.*
         |        FROM rcz_table a
         |          WHERE STATE IN (4,24) AND NOWTIME > '2023-01-02'
         |        ) a
         |    WHERE a.posdiff>=0
         |    GROUP BY BASEAREA,CRYSTALID,BGROUP,cspos_group
         |""".stripMargin
    val melt_phase_sql_DF = spark.sql(melt_phase_sql)
    melt_phase_sql_DF.createTempView("melt_phase_view")

    // 打锅转
    val CR_VIEW_sql =
      s"""
         |        SELECT t1.CRYSTALID,
         |            t1.BGROUP,
         |            t1.NOWTIME,
         |            t1.MELTSURFTEMP,
         |            t1.SETMELTSURFTEMP,
         |            (t1.MELTSURFTEMP - t1.SETMELTSURFTEMP) AS DIFF_MELTSURFTEMP,
         |            t1.CRUCIBLEROTATION,
         |            t1.LAST_CR
         |        FROM (SELECT t0.*,
         |            LAG(t0.CRUCIBLEROTATION, 1) OVER (PARTITION BY t0.BASEAREA, t0.CRYSTALID, t0.BGROUP ORDER BY t0.NOWTIME) AS LAST_CR
         |            FROM rcz_table t0
         |            ) t1
         |        WHERE t1.LAST_CR < 5 AND t1.CRUCIBLEROTATION > 4 AND t1.STATE IN (24)
         |""".stripMargin
    val CR_VIEW_sql_DF = spark.sql(CR_VIEW_sql)
    CR_VIEW_sql_DF.createTempView("CR_VIEW")

    // 放埚位
    val CP_VIEW_sql =
      """
        |            SELECT t2.CRYSTALID,
        |                t2.BGROUP,
        |                t2.NOWTIME,
        |                t2.CRUCIBLEPOS,
        |                t2.LAST_CP,
        |                t2.DIFF_CP,
        |                t2.rk
        |            FROM
        |                (SELECT t1.BASEAREA,
        |                t1.NOWTIME,
        |                t1.CRUCIBLEPOS,
        |                t1.CRYSTALID,
        |                t1.BGROUP,
        |                t1.LAST_CP,
        |                t1.DIFF_CP,
        |                row_number() OVER (PARTITION BY t1.BASEAREA, t1.CRYSTALID, t1.BGROUP ORDER BY t1.NOWTIME DESC) AS rk
        |                FROM (SELECT t0.*,
        |                LAG(t0.CRUCIBLEPOS, 1) OVER (PARTITION BY t0.BASEAREA, t0.CRYSTALID, t0.BGROUP ORDER BY t0.NOWTIME) AS LAST_CP,
        |                (t0.CRUCIBLEPOS - LAG(t0.CRUCIBLEPOS, 1) OVER (PARTITION BY t0.BASEAREA, t0.CRYSTALID, t0.BGROUP ORDER BY t0.NOWTIME)) AS DIFF_CP
        |                FROM rcz_table t0 WHERE t0.STATE IN (24)) t1
        |                WHERE t1.DIFF_CP != 0) t2
        |            WHERE t2.rk = 1
        |""".stripMargin

    val CP_VIEW_sql_DF = spark.sql(CP_VIEW_sql)
    CP_VIEW_sql_DF.createTempView("CP_VIEW")

    // 双目标
    val DUAL_TARGET_VIEW_sql =
      """
        |            SELECT t1.CRYSTALID,
        |                t1.BGROUP,
        |                t1.NOWTIME,
        |                t1.SETMAINHEATER,
        |                t1.SETMELTSURFTEMP
        |            FROM (
        |                SELECT t0.*,
        |                row_number() over (partition by t0.BASEAREA, t0.CRYSTALID, t0.BGROUP order by t0.NOWTIME) as rk
        |                FROM rcz_table t0
        |                WHERE t0.STATE = 5) t1
        |            WHERE t1.rk = 1
        |""".stripMargin

    val DUAL_TARGET_VIEW_sql_DF = spark.sql(DUAL_TARGET_VIEW_sql)
    DUAL_TARGET_VIEW_sql_DF.createTempView("DUAL_TARGET_VIEW")

    // 合并表
//    val MERGED_FEATURE_sql =
//      """
//        |    SELECT COALESCE(t0.CRYSTALID, t1.CRYSTALID, t2.CRYSTALID, t3.CRYSTALID) AS CRYSTALID,
//        |           COALESCE(t0.BGROUP, t1.BGROUP, t2.BGROUP, t3.BGROUP) AS BGROUP,
//        |           t0.NOWTIME AS CR_TIME,
//        |           t0.MELTSURFTEMP AS CR_MELTSURFTEMP,
//        |           t0.SETMELTSURFTEMP AS CR_SETMELTSURFTEMP,
//        |           t0.CRUCIBLEROTATION AS CR_CRUCIBLEROTATION,
//        |           t0.LAST_CR AS CR_LAST_CR,
//        |           t1.NOWTIME AS CP_TIME,
//        |           t1.CRUCIBLEPOS AS CP_CRUCIBLEPOS,
//        |           t1.LAST_CP AS CP_LAST_CP,
//        |           t1.DIFF_CP AS CP_DIFF_CP,
//        |           t2.NOWTIME AS DT_TIME,
//        |           t2.SETMAINHEATER AS DT_SETMAINHEATER,
//        |           t2.SETMELTSURFTEMP AS DT_SETMELTSURFTEMP,
//        |           t3.PHASE1_DURATION AS MELT_PHASE1_DURATION,
//        |           t3.PHASE2_DURATION AS MELT_PHASE2_DURATION,
//        |           t3.PHASE3_DURATION AS MELT_PHASE3_DURATION,
//        |           t3.PHASE4_DURATION AS MELT_PHASE4_DURATION,
//        |           t3.PHASE5_DURATION AS MELT_PHASE5_DURATION,
//        |           t3.PHASE1_MAX_MELTSURFTEMP AS MELT_PHASE1_MAX_MELTSURFTEMP,
//        |           t3.PHASE2_MAX_MELTSURFTEMP AS MELT_PHASE2_MAX_MELTSURFTEMP,
//        |           t3.PHASE3_MAX_MELTSURFTEMP AS MELT_PHASE3_MAX_MELTSURFTEMP,
//        |           t3.PHASE4_MAX_MELTSURFTEMP AS MELT_PHASE4_MAX_MELTSURFTEMP,
//        |           t3.PHASE5_MAX_MELTSURFTEMP AS MELT_PHASE5_MAX_MELTSURFTEMP,
//        |           t3.PHASE1_PHASE_POS AS MELT_PHASE1_PHASE_POS,
//        |           t3.PHASE2_PHASE_POS AS MELT_PHASE2_PHASE_POS,
//        |           t3.PHASE3_PHASE_POS AS MELT_PHASE3_PHASE_POS,
//        |           t3.PHASE4_PHASE_POS AS MELT_PHASE4_PHASE_POS,
//        |           t3.PHASE5_PHASE_POS AS MELT_PHASE5_PHASE_POS
//        |    FROM CR_VIEW t0  --准备调温打埚转
//        |    left JOIN
//        |    CP_VIEW t1 --准备调温放埚位
//        |    ON t0.CRYSTALID = t1.CRYSTALID AND t0.BGROUP = t1.BGROUP
//        |   left JOIN
//        |--     (SELECT *
//        |--         FROM (
//        |--         SELECT CRYSTALId, BGROUP, CSPOS_GROUP, DURATION, MAX_MELTSURFTEMP, PHASE_POS
//        |--             FROM melt_phase_view
//        |--             )
//        |--             PIVOT (
//        |--               MAX(DURATION) AS DURATION, MAX(MAX_MELTSURFTEMP) AS MAX_MELTSURFTEMP, MAX(PHASE_POS) AS PHASE_POS
//        |--               FOR CSPOS_GROUP IN ('0-200' AS PHASE1, '200-400' AS PHASE2, '400-600' AS PHASE3, '600-800' AS PHASE4, 'others' AS PHASE5)
//        |--                 )
//        |--     ) t3
//        |    (SELECT CRYSTALId,
//        |            BGROUP,
//        |            MAX(CASE WHEN CSPOS_GROUP = '0-200' THEN DURATION END) AS           PHASE1_DURATION,
//        |            MAX(CASE WHEN CSPOS_GROUP = '0-200' THEN MAX_MELTSURFTEMP END) AS   PHASE1_MAX_MELTSURFTEMP,
//        |            MAX(CASE WHEN CSPOS_GROUP = '0-200' THEN PHASE_POS END) AS          PHASE1_PHASE_POS,
//        |            MAX(CASE WHEN CSPOS_GROUP = '200-400' THEN DURATION END) AS         PHASE2_DURATION,
//        |            MAX(CASE WHEN CSPOS_GROUP = '200-400' THEN MAX_MELTSURFTEMP END) AS PHASE2_MAX_MELTSURFTEMP,
//        |            MAX(CASE WHEN CSPOS_GROUP = '200-400' THEN PHASE_POS END) AS        PHASE2_PHASE_POS,
//        |            MAX(CASE WHEN CSPOS_GROUP = '400-600' THEN DURATION END) AS         PHASE3_DURATION,
//        |            MAX(CASE WHEN CSPOS_GROUP = '400-600' THEN MAX_MELTSURFTEMP END) AS PHASE3_MAX_MELTSURFTEMP,
//        |            MAX(CASE WHEN CSPOS_GROUP = '400-600' THEN PHASE_POS END) AS        PHASE3_PHASE_POS,
//        |            MAX(CASE WHEN CSPOS_GROUP = '600-800' THEN DURATION END) AS         PHASE4_DURATION,
//        |            MAX(CASE WHEN CSPOS_GROUP = '600-800' THEN MAX_MELTSURFTEMP END) AS PHASE4_MAX_MELTSURFTEMP,
//        |            MAX(CASE WHEN CSPOS_GROUP = '600-800' THEN PHASE_POS END) AS        PHASE4_PHASE_POS,
//        |            MAX(CASE
//        |                    WHEN CSPOS_GROUP NOT IN ('0-200', '200-400', '400-600', '600-800')
//        |                        THEN DURATION END) AS                                   PHASE5_DURATION,
//        |            MAX(CASE
//        |                    WHEN CSPOS_GROUP NOT IN ('0-200', '200-400', '400-600', '600-800')
//        |                        THEN MAX_MELTSURFTEMP END) AS                           PHASE5_MAX_MELTSURFTEMP,
//        |            MAX(CASE
//        |                    WHEN CSPOS_GROUP NOT IN ('0-200', '200-400', '400-600', '600-800')
//        |                        THEN PHASE_POS END) AS                                  PHASE5_PHASE_POS
//        |     FROM (
//        |              SELECT CRYSTALId, BGROUP, CSPOS_GROUP, DURATION, MAX_MELTSURFTEMP, PHASE_POS
//        |              FROM melt_phase_view
//        |          ) subquery
//        |     GROUP BY CRYSTALId, BGROUP
//        |    ) t3  --准备调温及调温，籽晶预热及调温位置
//        |    ON t0.CRYSTALID = t3.CRYSTALID AND t0.BGROUP = t3.BGROUP
//        |    left JOIN
//        |    DUAL_TARGET_VIEW t2 --引晶双目标
//        |    ON t0.CRYSTALID = t2.CRYSTALID AND t0.BGROUP = t2.BGROUP
//        |""".stripMargin


    val MERGED_FEATURE_sql =
      """
        |SELECT COALESCE(t0.CRYSTALID, t2.CRYSTALID) AS CRYSTALID,
        |       COALESCE(t0.BGROUP, t2.BGROUP) AS BGROUP,
        |       t0.NOWTIME AS CR_TIME,
        |       t0.MELTSURFTEMP AS CR_MELTSURFTEMP,
        |       t0.SETMELTSURFTEMP AS CR_SETMELTSURFTEMP,
        |       t0.CRUCIBLEROTATION AS CR_CRUCIBLEROTATION,
        |       t0.LAST_CR AS CR_LAST_CR,
        |       t2.NOWTIME AS DT_TIME,
        |       t2.SETMAINHEATER AS DT_SETMAINHEATER,
        |       t2.SETMELTSURFTEMP AS DT_SETMELTSURFTEMP
        |FROM CR_VIEW t0
        |FULL JOIN
        |DUAL_TARGET_VIEW t2
        |ON t0.CRYSTALID = t2.CRYSTALID AND t0.BGROUP = t2.BGROUP
        |""".stripMargin

    val MERGED_FEATURE_sql_DF = spark.sql(MERGED_FEATURE_sql)
    // todo 上线前去掉
    //MERGED_FEATURE_sql_DF.show(10)
    MERGED_FEATURE_sql_DF.createTempView("MERGED_FEATURE")
    // todo 入库
    MERGED_FEATURE_sql_DF.printSchema()

    val MERGED_FEATURE_RES_sql =
      """
        |INSERT into TABLE test.agg_script_merged_feature_prd_new
        |SELECT * FROM MERGED_FEATURE
        |""".stripMargin
    spark.sql(MERGED_FEATURE_RES_sql)
    println("籽晶预热以及熔接位置  打锅转 放埚位 双目标 合并特征表 完成")
  }

  // 识别调温准备阶段编号   识别调温阶段编号   调温准备阶段升温斜率
  // 调温阶段降温斜率   调温阶段（state = 4），液面温度（MELTSURFTEMP）最高的点到目标功率不变的节点，计算平均斜率（平均变化速率：MELTSURFTEMP / NOWTIME)
  // 落表

  def agg_temp_slope(): Unit = {
    //识别调温准备阶段编号
    val rcz_with_tw_prep_phase_sql =
      """
        |    SELECT t2.*,
        |        SUM (t2.IS_TW_PREP_PHASE) OVER(PARTITION BY t2.CRYSTALID, t2.BGROUP ORDER BY t2.NOWTIME) AS NUM_TW_PREP_PHASE
        |    FROM
        |        (SELECT t1.*,
        |        CASE WHEN (t1.LAST_STATE NOT IN (24) AND t1.STATE IN (24)) THEN 1 ELSE 0 END AS IS_TW_PREP_PHASE
        |        FROM (SELECT t0.*,
        |        LAG(t0.STATE, 1) OVER(PARTITION BY t0.CRYSTALID, t0.BGROUP ORDER BY t0.nowtime) AS LAST_STATE
        |        FROM RCZ_TABLE t0) t1) t2
        |""".stripMargin
    val rcz_with_tw_prep_phase_sql_DF = spark.sql(rcz_with_tw_prep_phase_sql)
    rcz_with_tw_prep_phase_sql_DF.createTempView("rcz_with_tw_prep_phase")

    //识别调温阶段编号

    val rcz_with_tw_phase_sql =
      """
        |    SELECT t2.*,
        |        SUM (t2.IS_TW_PHASE) OVER(PARTITION BY t2.CRYSTALID, t2.BGROUP ORDER BY t2.NOWTIME) AS NUM_TW_PHASE
        |    FROM
        |        (SELECT t1.*,
        |        CASE WHEN (t1.LAST_STATE NOT IN (4) AND t1.STATE IN (4)) THEN 1 ELSE 0 END AS IS_TW_PHASE
        |        FROM (SELECT t0.*,
        |        LAG(t0.STATE, 1) OVER(PARTITION BY t0.CRYSTALID, t0.BGROUP ORDER BY t0.nowtime) AS LAST_STATE
        |        FROM RCZ_TABLE t0) t1) t2
        |""".stripMargin
    val rcz_with_tw_phase_sql_DF = spark.sql(rcz_with_tw_phase_sql)
    rcz_with_tw_phase_sql_DF.createTempView("rcz_with_tw_phase")

    //调温准备阶段升温斜率
    val temp_slope_table_1_sql =
      """
        |        SELECT t1.*,
        |            CASE WHEN (t1.MELTSURFTEMP = t1.MIN_MELTSURFTEMP) THEN 1 ELSE 0 END AS IS_MIN_TEMP
        |        FROM
        |            (
        |            SELECT
        |            t0.CRYSTALID,
        |            t0.BGROUP,
        |            t0.NUM_TW_PREP_PHASE,
        |            t0.STATE,
        |            t0.NOWTIME,
        |            t0.MELTSURFTEMP,
        |            t0.SETMELTSURFTEMP,
        |            MIN (t0.MELTSURFTEMP) OVER(PARTITION BY t0.CRYSTALID, t0.BGROUP, t0.NUM_TW_PREP_PHASE) AS MIN_MELTSURFTEMP,
        |            t0.MELTSURFTEMP - LAG(t0.MELTSURFTEMP, 1) OVER(PARTITION BY t0.CRYSTALID, t0.BGROUP, t0.NUM_TW_PREP_PHASE ORDER BY t0.NOWTIME) AS DIFF_MELTSURFTEMP
        |            FROM RCZ_WITH_TW_PREP_PHASE t0
        |            WHERE 1=1
        |            AND t0.MELTSURFTEMP >= 40 AND t0.MELTSURFTEMP <= 90
        |            AND t0.STATE IN (24)
        |            ) t1
        |        WHERE t1.DIFF_MELTSURFTEMP != 0
        |""".stripMargin

    val temp_slope_table_1_sql_DF = spark.sql(temp_slope_table_1_sql)
    temp_slope_table_1_sql_DF.createTempView("temp_slope_table_1")

    val temp_slope_table_2_sql =
      """
        |SELECT t2.*,
        |           (t2.MELTSURFTEMP - t2.LAST_MELTSURFTEMP) AS DELTA_MELTSURFTEMP,
        |          -- (t2.NOWTIME - t2.LAST_NOWTIME) * 24 * 60 AS DELTA_NOWTIME
        |        ( UNIX_TIMESTAMP(t2.NOWTIME) - UNIX_TIMESTAMP(t2.LAST_NOWTIME))/60  AS DELTA_NOWTIME
        |    FROM (SELECT t1.*,
        |                 LAG(t1.MELTSURFTEMP, 1) OVER (PARTITION BY t1.CRYSTALID, t1.BGROUP, t1.NUM_TW_PREP_PHASE ORDER BY t1.nowtime) AS LAST_MELTSURFTEMP,
        |                 LAG(t1.NOWTIME, 1) OVER (PARTITION BY t1.CRYSTALID, t1.BGROUP, t1.NUM_TW_PREP_PHASE ORDER BY t1.nowtime) AS LAST_NOWTIME
        |          FROM (SELECT t0.*,
        |                       SUM(t0.IS_MIN_TEMP)
        |                           OVER (PARTITION BY t0.CRYSTALID, t0.BGROUP, t0.NUM_TW_PREP_PHASE ORDER BY t0.NOWTIME) AS MIN_MELTSURFTEMP_GROUP
        |                FROM temp_slope_table_1 t0
        |                ) t1
        |          WHERE t1.MIN_MELTSURFTEMP_GROUP > 0) t2
        |""".stripMargin

    val temp_slope_table_2_sql_DF = spark.sql(temp_slope_table_2_sql)
    temp_slope_table_2_sql_DF.createTempView("temp_slope_table_2")

    val temp_slope_table_3_sql =
      """
        |        SELECT t0.*,
        |            (t0.DELTA_MELTSURFTEMP / t0.DELTA_NOWTIME) AS DELTA_SLOPE
        |        FROM temp_slope_table_2 t0
        |        WHERE t0.DELTA_NOWTIME   > 0
        |""".stripMargin

    val temp_slope_table_3_sql_DF = spark.sql(temp_slope_table_3_sql)
    temp_slope_table_3_sql_DF.createTempView("temp_slope_table_3")

    val temp_slope_table_4_sql =
      """
        |            SELECT CRYSTALID,
        |                BGROUP,
        |                NUM_TW_PREP_PHASE,
        |                SUM (DELTA_NOWTIME) AS TOTAL_TIME,
        |                AVG (SETMELTSURFTEMP) AS AVG_SETMELTSURFTEMP,
        |                MIN (MIN_MELTSURFTEMP) AS MIN_MIN_MELTSURFTEMP,
        |                AVG (DELTA_SLOPE) AS AVG_DELTA_SLOPE
        |            FROM temp_slope_table_3
        |            GROUP BY CRYSTALID, BGROUP, NUM_TW_PREP_PHASE
        |""".stripMargin

    val temp_slope_table_4_sql_DF = spark.sql(temp_slope_table_4_sql)
    temp_slope_table_4_sql_DF.createTempView("temp_slope_table_4")



    //调温阶段降温斜率
    //调温阶段（state = 4），液面温度（MELTSURFTEMP）最高的点到目标功率不变的节点，计算平均斜率（平均变化速率：MELTSURFTEMP / NOWTIME)

    val temp_down_slope_table_1_sql =
      """
        |        SELECT t1.*,
        |            CASE WHEN (t1.MELTSURFTEMP = t1.MAX_MELTSURFTEMP) THEN 1 ELSE 0 END AS IS_MAX_TEMP
        |        FROM
        |            (
        |            SELECT
        |            t0.CRYSTALID,
        |            t0.BGROUP,
        |            t0.NUM_TW_PHASE,
        |            t0.STATE,
        |            t0.NOWTIME,
        |            t0.MELTSURFTEMP,
        |            t0.SETMELTSURFTEMP,
        |            MAX (t0.MELTSURFTEMP) OVER(PARTITION BY t0.CRYSTALID, t0.BGROUP, t0.NUM_TW_PHASE) AS MAX_MELTSURFTEMP,
        |            t0.MELTSURFTEMP - LAG(t0.MELTSURFTEMP, 1) OVER(PARTITION BY t0.CRYSTALID, t0.BGROUP, t0.NUM_TW_PHASE ORDER BY t0.NOWTIME) AS DIFF_MELTSURFTEMP
        |            FROM RCZ_WITH_TW_PHASE t0
        |            WHERE 1=1
        |            AND t0.MELTSURFTEMP >= t0.SETMELTSURFTEMP + 2 AND t0.MELTSURFTEMP <= 100
        |            AND t0.STATE IN (4)
        |            ) t1
        |        WHERE t1.DIFF_MELTSURFTEMP != 0
        |""".stripMargin
    val temp_down_slope_table_1_sql_DF = spark.sql(temp_down_slope_table_1_sql)
    temp_down_slope_table_1_sql_DF.createTempView("temp_down_slope_table_1")

    val temp_down_slope_table_2_sql =
      """
        |            SELECT t2.*,
        |                (t2.MELTSURFTEMP - t2.LAST_MELTSURFTEMP) AS DELTA_MELTSURFTEMP,
        |                --(t2.NOWTIME - t2.LAST_NOWTIME) * 24 * 60 AS DELTA_NOWTIME
        |              --(CAST (t2.NOWTIME AS int) - CAST (t2.LAST_NOWTIME AS int) ) * 24 * 60 AS DELTA_NOWTIME
        |             ( UNIX_TIMESTAMP(t2.NOWTIME) - UNIX_TIMESTAMP(t2.LAST_NOWTIME))/60   AS DELTA_NOWTIME
        |            FROM (SELECT t1.*,
        |                LAG(t1.MELTSURFTEMP, 1) OVER (PARTITION BY t1.CRYSTALID, t1.BGROUP, t1.NUM_TW_PHASE ORDER BY t1.nowtime) AS LAST_MELTSURFTEMP,
        |                LAG(t1.NOWTIME, 1) OVER (PARTITION BY t1.CRYSTALID, t1.BGROUP, t1.NUM_TW_PHASE ORDER BY t1.nowtime) AS LAST_NOWTIME
        |                FROM (SELECT t0.*,
        |                SUM (t0.IS_MAX_TEMP)
        |                OVER (PARTITION BY t0.CRYSTALID, t0.BGROUP, t0.NUM_TW_PHASE ORDER BY t0.NOWTIME) AS MAX_MELTSURFTEMP_GROUP
        |                FROM temp_down_slope_table_1 t0) t1
        |                WHERE t1.MAX_MELTSURFTEMP_GROUP > 0) t2
        |""".stripMargin
    val temp_down_slope_table_2_sql_DF = spark.sql(temp_down_slope_table_2_sql)
    temp_down_slope_table_2_sql_DF.createTempView("temp_down_slope_table_2")

    val temp_down_slope_table_3_sql =
      """
        |        SELECT t0.*,
        |            (t0.DELTA_MELTSURFTEMP / t0.DELTA_NOWTIME) AS DELTA_SLOPE
        |        FROM temp_down_slope_table_2 t0
        |        WHERE t0.DELTA_NOWTIME > 0
        |""".stripMargin

    val temp_down_slope_table_3_sql_DF = spark.sql(temp_down_slope_table_3_sql)
    temp_down_slope_table_3_sql_DF.createTempView("temp_down_slope_table_3")

    val temp_down_slope_table_4_sql =
      """
        |        SELECT CRYSTALID,
        |            BGROUP,
        |            NUM_TW_PHASE,
        |            AVG (SETMELTSURFTEMP) AS AVG_SETMELTSURFTEMP,
        |            MAX (MAX_MELTSURFTEMP) AS MAX_MAX_MELTSURFTEMP,
        |            SUM (DELTA_NOWTIME) AS TOTAL_TIME,
        |            AVG (DELTA_SLOPE) AS AVG_DELTA_SLOPE
        |        FROM temp_down_slope_table_3
        |        GROUP BY CRYSTALID, BGROUP, NUM_TW_PHASE
        |""".stripMargin

    val temp_down_slope_table_4_sql_DF = spark.sql(temp_down_slope_table_4_sql)
    temp_down_slope_table_4_sql_DF.createTempView("temp_down_slope_table_4")

    // 落表
    val result_df_sql =
      """
        |        SELECT COALESCE(t0.CRYSTALID, t1.CRYSTALID) AS CRYSTALID,
        |               COALESCE(t0.BGROUP, t1.BGROUP) AS BGROUP,
        |               t0.NUM_TW_PREP_PHASE,
        |               t0.TOTAL_TIME AS UP_DURATION,
        |               t0.AVG_SETMELTSURFTEMP AS UP_SETMELTSURFTEMP,
        |               t0.MIN_MIN_MELTSURFTEMP AS UP_MIN_MELTSURFTEMP,
        |               t0.AVG_DELTA_SLOPE AS AVG_UP_SLOPE,
        |               t1.NUM_TW_PHASE,
        |               t1.TOTAL_TIME AS DOWN_DURATION,
        |               t1.AVG_SETMELTSURFTEMP AS DOWN_SETMELTSURFTEMP,
        |               t1.MAX_MAX_MELTSURFTEMP AS DOWN_MAX_MELTSURFTEMP,
        |               t1.AVG_DELTA_SLOPE AS AVG_DOWN_SLOPE
        |        FROM temp_slope_table_4 t0 ---调温准备阶段升温斜率
        |       left  JOIN
        |        temp_down_slope_table_4 t1  ---调温准备阶段降温斜率
        |        ON t0.CRYSTALID = t1.CRYSTALID AND t0.BGROUP = t1.BGROUP
        |""".stripMargin

    val result_df_sql_DF = spark.sql(result_df_sql)
    result_df_sql_DF.createTempView("result_df_sql_DF_view")
    // todo 最后注释掉
    //result_df_sql_DF.show()
    result_df_sql_DF.printSchema()

    val MERGED_FEATURE_RES_sql =
      """
        |INSERT OVERWRITE TABLE test.agg_temp_slope_prd
        |SELECT * FROM result_df_sql_DF_view
        |""".stripMargin
    spark.sql(MERGED_FEATURE_RES_sql)

    // todo 入库
    println("agg_temp_slope 入库完成")
  }


  //识别调温准备阶段编号
  //换热器位置
  def agg_lower_he(): Unit = {

    val rcz_with_tw_prep_phase_sql =
      s"""
         |SELECT t2.*,
         |        SUM(t2.IS_TW_PREP_PHASE) OVER(PARTITION BY t2.CRYSTALID, t2.BGROUP ORDER BY t2.NOWTIME) AS NUM_TW_PREP_PHASE
         |    FROM
         |        (SELECT t1.*,
         |                CASE WHEN (t1.LAST_STATE NOT IN (24) AND t1.STATE IN (24)) THEN 1 ELSE 0 END AS IS_TW_PREP_PHASE
         |        FROM (SELECT t0.*,
         |                     LAG(t0.STATE, 1) OVER(PARTITION BY t0.CRYSTALID, t0.BGROUP ORDER BY t0.nowtime) AS LAST_STATE
         |              FROM rcz_table t0) t1) t2
         |""".stripMargin

    val rcz_with_tw_prep_phase_sql_DF = spark.sql(rcz_with_tw_prep_phase_sql)
    rcz_with_tw_prep_phase_sql_DF.createTempView("rcz_with_tw_prep_phase_agg_lower_he")

    val agg_lower_he_res_sql =
      """
        |    SELECT a.BASEAREA,
        |           a.CRYSTALId,
        |           a.BGROUP,
        |           a.NUM_TW_PREP_PHASE,
        |           a.hepos_group,
        |           min(a.NOWTIME) AS HE_PHASE_START_TIME,
        |           max(a.NOWTIME) AS HE_PHASE_END_TIME,
        |           sum(a.timediff) AS DURATION,
        |           max(HEATEXCHANGERPOS) AS MAX_HEATEXCHANGERPOS,
        |           min(HEATEXCHANGERPOS) AS MIN_HEATEXCHANGERPOS,
        |           percentile(a.HEATEXCHANGERPOS, 0.5) AS PHASE_POS
        |    FROM (
        |        SELECT
        |               --(NOWTIME-lag(NOWTIME,1) over(partition by BASEAREA,CRYSTALID,BGROUP,NUM_TW_PREP_PHASE order by NOWTIME))*24*60 as timediff,
        |               (unix_timestamp(NOWTIME)- unix_timestamp(lag(NOWTIME,1) over(partition by BASEAREA,CRYSTALID,BGROUP,NUM_TW_PREP_PHASE order by NOWTIME)))/60 as timediff,
        |               HEATEXCHANGERPOS - lag(HEATEXCHANGERPOS,1) over(partition by BASEAREA,CRYSTALID,BGROUP,NUM_TW_PREP_PHASE order by NOWTIME) as posdiff,
        |               case
        |                   when HEATEXCHANGERPOS>=0 and HEATEXCHANGERPOS<=6 then '0-6'
        |                   when HEATEXCHANGERPOS>6 and HEATEXCHANGERPOS<=100 then '6-100'
        |                   when HEATEXCHANGERPOS>100 and HEATEXCHANGERPOS<=200 then '100-200'
        |                   when HEATEXCHANGERPOS>200 and HEATEXCHANGERPOS<=400 then '200-400'
        |                   when HEATEXCHANGERPOS>400 then '400+'
        |               end hepos_group,
        |               a.*
        |        FROM rcz_with_tw_prep_phase_agg_lower_he a
        |        WHERE STATE IN (24)
        |        ) a
        |    WHERE a.posdiff>=0 AND NUM_TW_PREP_PHASE = 1
        |    GROUP BY BASEAREA,CRYSTALID,BGROUP,NUM_TW_PREP_PHASE,hepos_group
        |""".stripMargin

    val agg_lower_he_res_sql_DF = spark.sql(agg_lower_he_res_sql)
    agg_lower_he_res_sql_DF.createTempView("agg_lower_he_res_sql_DF_view")
    agg_lower_he_res_sql_DF.printSchema()

    val insertSql =
      """
        |INSERT OVERWRITE TABLE test.agg_lower_he
        |SELECT * FROM agg_lower_he_res_sql_DF_view
        |""".stripMargin
    spark.sql(insertSql)
    println("agg_lower_he 入库完成")
  }


  //agg_cp_detail
  def agg_cp_detail(): Unit ={
    val rcz_with_tw_prep_phase_sql =
      """
        |SELECT t2.*,
        |                 SUM(t2.IS_TW_PREP_PHASE) OVER(PARTITION BY t2.CRYSTALID, t2.BGROUP ORDER BY t2.NOWTIME) AS NUM_TW_PREP_PHASE
        |          FROM (SELECT t1.*,
        |                       CASE WHEN (t1.LAST_STATE NOT IN (24) AND t1.STATE IN (24)) THEN 1 ELSE 0 END AS IS_TW_PREP_PHASE
        |                FROM (SELECT t0.*,
        |                             LAG(t0.STATE, 1) OVER(PARTITION BY t0.CRYSTALID, t0.BGROUP ORDER BY t0.nowtime) AS LAST_STATE
        |                      FROM RCZ_TABLE t0) t1) t2
        |""".stripMargin

    val rcz_with_tw_prep_phase_sql_DF= spark.sql(rcz_with_tw_prep_phase_sql)

    rcz_with_tw_prep_phase_sql_DF.createTempView("rcz_with_tw_prep_phase_agg_cp_detail")

    val res_sql=
      """
        |   SELECT t2.CRYSTALID,
        |       t2.BGROUP,
        |       t2.NUM_TW_PREP_PHASE,
        |       t2.NOWTIME,
        |       t2.CRUCIBLEPOS,
        |       t2.LAST_CP,
        |       t2.DIFF_CP,
        |       t2.rk
        |   FROM
        |    (SELECT t1.*,
        |            row_number() OVER (PARTITION BY t1.BASEAREA, t1.CRYSTALID, t1.BGROUP, t1.NUM_TW_PREP_PHASE ORDER BY t1.NOWTIME DESC) AS rk
        |    FROM (SELECT t0.*,
        |                 LAG(t0.CRUCIBLEPOS, 1) OVER (PARTITION BY t0.BASEAREA, t0.CRYSTALID, t0.BGROUP, t0.NUM_TW_PREP_PHASE ORDER BY t0.NOWTIME) AS LAST_CP,
        |                 (t0.CRUCIBLEPOS - LAG(t0.CRUCIBLEPOS, 1) OVER (PARTITION BY t0.BASEAREA, t0.CRYSTALID, t0.BGROUP, t0.NUM_TW_PREP_PHASE ORDER BY t0.NOWTIME)) AS DIFF_CP
        |          FROM rcz_with_tw_prep_phase_agg_cp_detail t0 WHERE t0.STATE IN (24)) t1
        |    WHERE t1.DIFF_CP >= 3 OR t1.DIFF_CP <= -3) t2
        |   WHERE t2.rk = 1
        |""".stripMargin

    val res_sql_DF= spark.sql(res_sql)
    res_sql_DF.createTempView("agg_cp_detail_res")
    val insertSql =
      """
        |INSERT OVERWRITE TABLE test.agg_cp_detail
        |SELECT * FROM agg_cp_detail_res
        |""".stripMargin
    spark.sql(insertSql)
    println("agg_cp_detail 入库完成")


  }
}
