package warehouse.dwd.scada.rcz

import org.apache.spark.sql.functions.{col, substring}
import org.apache.spark.storage.StorageLevel
import util.SparkUtil

/*
此类为中控数据整炉数据、rcz分组、各个工步分组数据，所有业务可以共用
su - liangwt -c "spark-submit --class warehouse.dwd.scada.rcz.RCZParser --master yarn --deploy-mode client --driver-memory 6g --num-executors 10 --executor-memory 4g --executor-cores 4 --queue spark --conf spark.shuffle.service.enabled=true --conf spark.sql.session.timeZone=Asia/Shanghai --conf spark.sql.catalogImplementation=hive --name RCZParser --jars /home/liangwt/jar/hutool-core-5.8.18.jar,/home/liangwt/jar/hutool-json-5.8.18.jar /home/liangwt/jar/silicon_data_flow-1.0-SNAPSHOT-dependencies.jar"
 */
object RCZParser {
  val spark = SparkUtil.hiveSparkSession("RCZParser")

  def main(args: Array[String]): Unit = {

    //step1 整炉数据、数据筛选、异常处理等
    ods_parser
    //step2 停炉分组
    tlgroup_parser
    //step3 新晶编分组
    newcrystalid_parser
    //step4 rzc 分组
    rcz_parser
    //step5 工步操作分组
    opGrpup_parser
    //入库
    result_sink
    spark.stop()
  }

  //整炉数据、数据筛选、异常处理等
  def  ods_parser(): Unit = {
    val odsParser_sql =
      s"""
         |    SELECT basearea,
         |        crystalid,
         |        nowtime,
         |        state,
         |        diameter, --直径
         |        setdiameter,
         |        mainheater,
         |        crystallength, --长度
         |        setmainheater,
         |        cruciblelift, -- 坩埚升速
         |        bottomheater,                     --底加功率
         |        substr(basearea, 1, 2) as base,   --基地
         |        SUBSTR(BASEAREA, -3)   as puller, --炉号
         |        setbottomheater,                  --设定底加功率
         |        meltsurftemp, --开始页面亮度
         |        setmeltsurftemp, --开始目标页面亮度
         |        avgslspeed , --平均拉速
         |        cruciblepos, --埚位
         |        meltlevel, --液口距
         |        case when crystalweight<0 then 0 else crystalweight end as crystalweight , --晶体重量数据做了修正
         |        case when state in(0,26,25) or mainheater <15  then 0
         |            else state end as nstate --停炉开始  这里必须去除掉很多并非停炉的数据
         |
         | FROM ods_ingot_scada.djzk_app_result
         | WHERE
         | day_id >= '2023-08-24'
         | and day_id <= '2023-09-24'
         | and crystalid !='DS'
         | and length (crystalid)>=10
         | --and left (SUBSTR(BASEAREA, -3),1) in ('A','B','C','D')
         | and left (BASEAREA, 2) in ('ES','GF')
         |  --and mainheater > 10  --实际主加功率,这里小于10的数据处于停炉状态
         |  --and BASEAREA = 'QJ_A94'
         |  --and crystalid = 'ZQS37A9302'
         |-- and  substr(basearea, 1, 2)='QJ'
         |""".stripMargin

    val odsParser_sql_DF = spark.sql(odsParser_sql)
    odsParser_sql_DF.createTempView("ods")
    println("ods_parser 完成")
  }

  //停炉分组
  def tlgroup_parser(): Unit = {
    val tlgroup_parser_sql =
      s"""
         |select
         |sum(tl_start) over(partition by basearea order by nowtime) as total_group, --整炉分组
         |* from
         |(
         |--这里要剔除手动、异常的停炉数据（）
         |select case when nstate=0 and laststate!=0 and laststate!=1  and mainheater>15 then 1 else 0 end as tl_start,
         |  *
         |  from
         |(select  lag(nstate, 1) over(partition by basearea order by nowtime) as laststate,
         |        lead(nstate, 1) over(partition by basearea order by nowtime) as leadstate,
         |         *
         |from ods)AL)BL
         |""".stripMargin
    val tlgroup_parser_sql_DF = spark.sql(tlgroup_parser_sql)
    tlgroup_parser_sql_DF.createTempView("tlgroup")

    println("tlgroup_parser 完成")
  }

  //新晶编分组
  def newcrystalid_parser(): Unit = {
    val newcrystalid_parser_sql =
      """
        |select
        |first_value(crystalid) over(partition by basearea,total_group order  by nowtime desc) as new_crystalid, --取最后一位作为晶编，一般比较准确
        |* from
        |tlgroup
        |where total_group>0 --过滤头部数据
        |""".stripMargin

    val newcrystalid_parser_sql_DF = spark.sql(newcrystalid_parser_sql)
    newcrystalid_parser_sql_DF.createTempView("newcrystalid")
    println("newcrystalid_parser 完成")

  }

  //rzc 分组
  def rcz_parser(): Unit = {
    val rcz_parser_sql =
      """
        |select
        |        SUM(ISOPENBOT) OVER(partition by basearea,total_group,new_crystalid order by nowtime) BGROUP,     ---- RCZ段数
        |* from
        |(select
        |CASE WHEN (MAXSTATE IN (4, 5, 6, 7, 8, 9)) THEN XISOPENBOT ELSE 0 END ISOPENBOT,
        |*
        |from
        |(SELECT
        |MAX(fill_state) OVER(PARTITION BY basearea,total_group,new_crystalid, XBGROUP) MAXSTATE,
        |*  --CL
        |from
        |(select
        |        SUM(xisopenbot) OVER(PARTITION BY basearea,total_group,new_crystalid ORDER BY nowtime) XBGROUP,   -- RCZ
        |*
        |from
        |(select
        |case when ((last_sbheater + last_smheater < 130)
        |              and (setbottomheater + setmainheater >= 130)) then 1 else 0 end xisopenbot, -- 是否开底加
        |case when ((nstate > 4) and (nstate <= 9)) then nstate else 0 end fill_state,
        |       * from
        |(select
        |        lag(setbottomheater, 1)   over(partition BY basearea,total_group, new_crystalid order by nowtime) as last_sbheater, --设定底加功率lag
        |        lag(setmainheater, 1) over(partition BY basearea,total_group, new_crystalid order by nowtime) as last_smheater,   --设定主加功率lag
        |        lag(mainheater, 1) over(partition BY basearea,total_group, new_crystalid order by nowtime) as last_mheater,       --设定主加功率lag
        |        lag(bottomheater, 1) over(partition BY basearea,total_group, new_crystalid order by nowtime) as last_bheater,
        |       * from
        |newcrystalid)AL)BL)CL)DL)EL
        |""".stripMargin

    val rcz_parser_sql_DF = spark.sql(rcz_parser_sql)
    rcz_parser_sql_DF.persist(StorageLevel.MEMORY_AND_DISK)
    rcz_parser_sql_DF.createTempView("rcz_group")
    println("rcz_parser 完成")

  }

  def opGrpup_parser(): Unit = {
    val opGrpup_parser_sql =
      """
        |select  *,
        |        SUM(ISSE) OVER(PARTITION BY basearea,total_group,new_crystalid, bgroup order by nowtime) as SEGP,    -- 引晶分组  --后续使用的时候记得只拿>0的数据
        |        SUM(ISSH) OVER(PARTITION BY basearea,total_group,new_crystalid, bgroup order by nowtime) as SHGP,    -- 放肩分组
        |        SUM(ISTU) OVER(PARTITION BY basearea,total_group,new_crystalid, bgroup order by nowtime) as TUGP,    -- 转肩分组
        |        SUM(ISME) OVER(PARTITION BY basearea,total_group,new_crystalid, bgroup order by nowtime) as MEGP,    -- 等径分组
        |        SUM(ISEN) OVER(PARTITION BY basearea,total_group,new_crystalid, bgroup order by nowtime) as ENGP,    -- 收尾分组
        |        SUM(ISCB) OVER(PARTITION BY basearea,total_group,new_crystalid, bgroup order by nowtime) as CBGP    -- 关底加分组
        |        from
        |(SELECT *,
        |       CASE WHEN max_XSEGP IN (4, 5, 6, 7, 8, 9) THEN XISSE ELSE 0 END ISSE,
        |       CASE WHEN max_XSHGP IN (4, 5, 6, 7, 8, 9) THEN XISSH ELSE 0 END ISSH,
        |       CASE WHEN max_XTUGP IN (4, 5, 6, 7, 8, 9) THEN XISTU ELSE 0 END ISTU,
        |       CASE WHEN max_XMEGP IN (4, 5, 6, 7, 8, 9) THEN XISME ELSE 0 END ISME,
        |       CASE WHEN max_XENGP IN (4, 5, 6, 7, 8, 9) THEN XISEN ELSE 0 END ISEN,
        |       CASE WHEN max_XCBGP IN (4, 5, 6, 7, 8, 9) THEN XISCB ELSE 0 END ISCB
        |       from
        |(SELECT *,
        |       MAX(fill_state) OVER(PARTITION BY basearea,total_group,new_crystalid, BGROUP, XSEGP) as max_XSEGP,
        |       MAX(fill_state) OVER(PARTITION BY basearea,total_group,new_crystalid, BGROUP, XSHGP) as max_XSHGP,
        |       MAX(fill_state) OVER(PARTITION BY basearea,total_group,new_crystalid, BGROUP, XTUGP) as max_XTUGP,
        |       MAX(fill_state) OVER(PARTITION BY basearea,total_group,new_crystalid, BGROUP, XMEGP) as max_XMEGP,
        |       MAX(fill_state) OVER(PARTITION BY basearea,total_group,new_crystalid, BGROUP, XENGP) as max_XENGP,
        |       MAX(fill_state) OVER(PARTITION BY basearea,total_group,new_crystalid, BGROUP,XCBGP) as max_XCBGP
        |       from
        |(select
        |*,
        |       -- 这里在开始按照rcz分组进行分组
        |       SUM(XISTE) OVER(PARTITION BY basearea,total_group, new_crystalid,bgroup ORDER BY nowtime) TEGP,   -- 调温分组
        |       SUM(XISSE) OVER(PARTITION BY basearea,total_group, new_crystalid,bgroup ORDER BY nowtime) XSEGP,
        |       SUM(XISSH) OVER(PARTITION BY basearea,total_group, new_crystalid,bgroup ORDER BY nowtime) XSHGP,
        |       SUM(XISTU) OVER(PARTITION BY basearea,total_group, new_crystalid,bgroup ORDER BY nowtime) XTUGP,
        |       SUM(XISME) OVER(PARTITION BY basearea,total_group, new_crystalid,bgroup ORDER BY nowtime) XMEGP,
        |       SUM(XISEN) OVER(PARTITION BY basearea,total_group, new_crystalid,bgroup ORDER BY nowtime) XENGP,
        |       SUM(XISCB) OVER(PARTITION BY basearea,total_group, new_crystalid,bgroup ORDER BY nowtime) XCBGP   -- 关底加分组
        |       from
        |(select
        |*,
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
        |        CASE WHEN ((last_sbheater + last_smheater >= 130)
        |        and (setbottomheater + setmainheater < 130)) THEN 1 ELSE 0 END  XISCB  -- 关底加分组
        |from rcz_group where BGROUP > 0)AL)BL)CL)DL
        |""".stripMargin

    val opGrpup_parser_sql_DF = spark.sql(opGrpup_parser_sql)
    opGrpup_parser_sql_DF.persist(StorageLevel.MEMORY_AND_DISK)
    opGrpup_parser_sql_DF.createTempView("opGrpup")


    val stepgroup_sql =
      s"""
         |select * from
         |(select
         | *,
         |           case when nstate = 8 then MEGP else 0 end stepno8,   -- 等径
         |           case when nstate = 9 then ENGP else 0 end stepno9,   -- 收尾
         |           case when nstate = 7 then TUGP else 0 end stepno7,   -- 转肩
         |           case when nstate = 6 then SHGP else 0 end stepno6,   -- 放肩
         |           case when nstate = 5 then SEGP else 0 end stepno5,   -- 引晶
         |           case when nstate = 4 then TEGP else 0 end stepno4,   -- 调温
         |
         |           case when TEGP = 0 then CBGP else 0 end stepno3,    -- 预调温
         |           case when (nstate != 3) and (CBGP=0) then bgroup else 0 end stepno2,  -- 化料（不包含熔料）
         |
         |           case when nstate = 3 then 1 else null end as xishead    -- 是否首段
         |           from opGrpup)AL
         |where stepno2 > 0 or stepno3 >0 or stepno4 >0
         |   or stepno5 >0 or stepno6 >0 or stepno7 >0
         |   or stepno8 >0 or stepno9 >0
         |""".stripMargin
    val stepgroup_sql_DF = spark.sql(stepgroup_sql)
      .withColumn("day_id", substring(col("nowtime"), 1, 11))
    stepgroup_sql_DF.createTempView("stepgroup")
    stepgroup_sql_DF.printSchema()

    println("opGrpup_parser 完成")

  }

  def result_sink(): Unit = {

    val result_sink_sql =
      """
        |INSERT OVERWRITE TABLE test.rcz_pasre partition(day_id)
        |SELECT * FROM stepgroup
        |""".stripMargin
    spark.sql(result_sink_sql)
    println("result_sink 完成")

  }


}
