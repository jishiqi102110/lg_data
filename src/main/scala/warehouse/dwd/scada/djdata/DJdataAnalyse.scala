package warehouse.dwd.scada.djdata

import org.apache.spark.sql.functions.{col, substring}
import util.SparkUtil

/*
此类主要计算单晶数据需求相关数据,会先进行rzc分组，分完组之后进行放肩以及熔料
数据取曲靖过去三个月数据
su - liangwt -c "spark-submit --class warehouse.dwd.scada.djdata.DJdataAnalyse --master yarn --deploy-mode client --driver-memory 6g --num-executors 8 --executor-memory 6g --executor-cores 4 --queue spark --conf spark.shuffle.service.enabled=true --conf spark.sql.session.timeZone=Asia/Shanghai --conf spark.sql.catalogImplementation=hive --name DJdataAnalyse --jars /home/liangwt/jar/hutool-core-5.8.18.jar,/home/liangwt/jar/hutool-json-5.8.18.jar /home/liangwt/jar/silicon_data_flow-1.0-SNAPSHOT-dependencies.jar"

 */
object DJdataAnalyse {
  def main(args: Array[String]): Unit = {

    val spark = SparkUtil.hiveSparkSession("DJdataAnalyse")
    import spark.implicits._
    if(args.size ==2){
      val start_day = args(0)
      val end_day = args(1)
      println(s"start_day :$start_day")
      println(s"end_day :$end_day")
    }

    // step1 rcz 分组
    val bggroupSql=
      s"""
      | select * from
      |(select *,
      |           case when state = 8 then MEGP else 0 end stepno8,   -- 等径
      |           case when state = 9 then ENGP else 0 end stepno9,   -- 收尾
      |           case when state = 7 then TUGP else 0 end stepno7,   -- 转肩
      |           case when state = 6 then SHGP else 0 end stepno6,   -- 放肩
      |           case when state = 5 then SEGP else 0 end stepno5,   -- 引晶
      |           case when state = 4 then TEGP else 0 end stepno4,   -- 调温
      |           case when TEGP = 0 then CBGP else 0 end stepno3,    -- 预调温
      |           case when (state != 3) and (CBGP=0) then bgroup else 0 end stepno2  -- 化料（不包含熔料）
      |           , case when state = 3 then 1 else null end as xishead    -- 是否首段
      |           from
      |(select basearea, --JL
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
      |        base,   --基地
      |        puller, --炉号
      |        setbottomheater,                  --设定底加功率
      |        meltsurftemp, --开始页面亮度
      |        crystalweight, --晶体重量
      |        setmeltsurftemp, --开始目标页面亮度
      |        avgslspeed , --平均拉速
      |        cruciblepos, --埚位
      |        meltlevel, --液口距
      |        lag(setbottomheater, 1)   over(partition BY basearea, crystalid order by nowtime) as last_sbheater, --设定底加功率lag
      |        lag(setmainheater, 1) over(partition BY basearea, crystalid order by nowtime) as last_smheater,   --设定主加功率lag
      |        lag(mainheater, 1) over(partition BY basearea, crystalid order by nowtime) as last_mheater,       --设定主加功率lag
      |        lag(bottomheater, 1) over(partition BY basearea, crystalid order by nowtime) as last_bheater,     --设定主加功率lag
      |        lag(nowtime, 1) over(partition BY basearea, crystalid order by nowtime) as lasttime,
      |        lag(state, 1) over(partition by basearea, crystalid order by nowtime) as laststate ,              --换热器位置
      |        TEGP,-- 调温分组
      |        SUM(ISSE) OVER(PARTITION BY basearea, bgroup order by nowtime) as SEGP,    -- 引晶分组  --后续使用的时候记得只拿>0的数据
      |        SUM(ISSH) OVER(PARTITION BY basearea, bgroup order by nowtime) as SHGP,    -- 放肩分组
      |        SUM(ISTU) OVER(PARTITION BY basearea, bgroup order by nowtime) as TUGP,    -- 转肩分组
      |        SUM(ISME) OVER(PARTITION BY basearea, bgroup order by nowtime) as MEGP,    -- 等径分组
      |        SUM(ISEN) OVER(PARTITION BY basearea, bgroup order by nowtime) as ENGP,    -- 收尾分组
      |        SUM(ISCB) OVER(PARTITION BY basearea, bgroup order by nowtime) as CBGP,    -- 关底加分组
      |        BGROUP   -- 开底加分组
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
      |       MAX(fill_state) OVER(PARTITION BY basearea, BGROUP, XSEGP) as max_XSEGP,
      |       MAX(fill_state) OVER(PARTITION BY basearea, BGROUP, XSHGP) as max_XSHGP,
      |       MAX(fill_state) OVER(PARTITION BY basearea, BGROUP, XTUGP) as max_XTUGP,
      |       MAX(fill_state) OVER(PARTITION BY basearea, BGROUP, XMEGP) as max_XMEGP,
      |       MAX(fill_state) OVER(PARTITION BY basearea, BGROUP, XENGP) as max_XENGP,
      |       MAX(fill_state) OVER(PARTITION BY basearea, BGROUP, XCBGP) as max_XCBGP
      |       from
      |(SELECT *,
      |       -- 这里在开始按照rcz分组进行分组
      |       SUM(XISTE) OVER(PARTITION BY basearea, bgroup ORDER BY nowtime) TEGP,   -- 调温分组
      |       SUM(XISSE) OVER(PARTITION BY basearea, bgroup ORDER BY nowtime) XSEGP,
      |       SUM(XISSH) OVER(PARTITION BY basearea, bgroup ORDER BY nowtime) XSHGP,
      |       SUM(XISTU) OVER(PARTITION BY basearea, bgroup ORDER BY nowtime) XTUGP,
      |       SUM(XISME) OVER(PARTITION BY basearea, bgroup ORDER BY nowtime) XMEGP,
      |       SUM(XISEN) OVER(PARTITION BY basearea, bgroup ORDER BY nowtime) XENGP,
      |       SUM(XISCB) OVER(PARTITION BY basearea, bgroup ORDER BY nowtime) XCBGP   -- 关底加分组
      |       from
      |(select *,
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
      |from
      |(SELECT *, --EL
      |        SUM(ISOPENBOT) OVER(partition by basearea order by nowtime) BGROUP     ---- RCZ段数
      |        from
      |(select *,
      |        CASE WHEN (MAXSTATE IN (4, 5, 6, 7, 8, 9)) THEN XISOPENBOT ELSE 0 END ISOPENBOT
      |        from
      |(SELECT *,  --CL
      |         MAX(fill_state) OVER(PARTITION BY basearea, XBGROUP) MAXSTATE
      |         from
      |(SELECT *,
      |        SUM(xisopenbot) OVER(PARTITION BY basearea ORDER BY nowtime) XBGROUP   -- RCZ
      |        from
      |(select * ,
      |         case when ((last_sbheater + last_smheater < 120)
      |              and (setbottomheater + setmainheater >= 120)) then 1 else 0 end xisopenbot, -- 是否开底加
      |              case when ((state > 4) and (state <= 9)) then state else 0 end fill_state
      |              from
      |(SELECT basearea,
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
      |        case when crystalweight<0 then 0 else crystalweight end as crystalweight , --晶体重量
      |        lag(setbottomheater, 1)   over(partition BY basearea, crystalid order by nowtime) as last_sbheater, --设定底加功率lag
      |        lag(setmainheater, 1) over(partition BY basearea, crystalid order by nowtime) as last_smheater,   --设定主加功率lag
      |        lag(mainheater, 1) over(partition BY basearea, crystalid order by nowtime) as last_mheater,       --设定主加功率lag
      |        lag(bottomheater, 1) over(partition BY basearea, crystalid order by nowtime) as last_bheater,     --设定主加功率lag
      |        lag(nowtime, 1) over(partition BY basearea, crystalid order by nowtime) as lasttime,
      |        lag(state, 1) over(partition by basearea, crystalid order by nowtime) as laststate,              --换热器位置
      |        row_number() over(partition by basearea order by crystalid) as rank_num
      | FROM ods_ingot_scada.djzk_app_result
      | WHERE
      |    day_id >= '2023-01-01'
      |   and day_id <= '2023-08-10'
      |   and crystalid !='DS'
      |  and length (crystalid)>=10
      |  and mainheater > 10  --实际主加功率
      |--   and BASEAREA = 'GF_B96'
      |--  and crystalid = 'BBS38B5401'
      |   and  substr(basearea, 1, 2)='QJ'
      |)AL
      |where rank_num>1
      |    )
      |BL)CL)DL)EL)FL)GL)HL)IL)JL)KL)LL
      |where stepno6 >0
      | """.stripMargin

    val bggroupSqlDF= spark.sql(bggroupSql)

    bggroupSqlDF.createTempView("bggroupSqlDF_view")


    // step2 放肩分组及指标计算

    val fjSql =
      s"""
         |select
         |       basearea,
         |        crystalid,
         |        nowtime,
         |        state,
         |       start_diff_surftemp, ----放肩开始与目标的亮度偏差
         |       fj_duration, -- 放肩时长
         |       fj_start_crystalweight, --放肩起始重量
         |       fj_end_crystalweight, --放肩结束重量
         |       -- 放肩
         |       avg_avgslspeed,--平均拉速
         |       avg_diameter,--平均直径
         |       avg_cruciblelift,--平均埚升
         |       avg_meltlevel,--平均液口距
         |       avg_cruciblepos,--平均埚位
         |       BGROUP,
         |       stepno6,
         |      0_100_avgslspeed_rate,  -- 0_100拉速变化率
         |      100_200_avgslspeed_rate,  -- 100_200拉速变化率
         |      200_300_avgslspeed_rate,  -- 200_300拉速变化率
         |
         |      0_100_diameter_rate,  -- 0_100直径变化率
         |      100_200_diameter_rate,  -- 100_200直径变化率
         |      200_300_diameter_rate,  -- 200_300直径变化率
         |
         |      0_100_shoulder_rate, -- 0_100肩形变化率
         |      100_200_shoulder_rate, -- 100_200肩形变化率
         |      200_300_shoulder_rate -- 200_300肩形变化率
         |from
         |(select *,
         |       round(0_100_diff_crystallength/0_100_duration,4) as 0_100_avgslspeed_rate,  -- 0_100拉速变化率
         |       round(0_100_diff_diameter/0_100_duration,4) as 0_100_diameter_rate,  -- 0_100直径变化率
         |       round((0_100_diff_diameter/0_100_diff_crystallength)/0_100_duration,4) as 0_100_shoulder_rate, -- 0_100肩形变化率
         |
         |       round(100_200_diff_crystallength/100_200_duration,4) as 100_200_avgslspeed_rate,  -- 100_200拉速变化率
         |       round(100_200_diff_diameter/100_200_duration,4) as 100_200_diameter_rate,  -- 100_200直径变化率
         |       round((100_200_diff_diameter/100_200_diff_crystallength)/100_200_duration,4) as 100_200_shoulder_rate, -- 100_200肩形变化率
         |
         |       round(200_300_diff_crystallength/200_300_duration,4) as 200_300_avgslspeed_rate,  -- 200_300拉速变化率
         |       round(200_300_diff_diameter/200_300_duration,4) as 200_300_diameter_rate,  -- 200_300直径变化率
         |       round((200_300_diff_diameter/200_300_diff_crystallength)/200_300_duration,4) as 200_300_shoulder_rate, -- 200_300肩形变化率
         |       row_number() over(partition by basearea,crystalid,BGROUP,stepno6  order by nowtime) as rank_num
         |       from
         |(select * ,
         |(unix_timestamp(max(x0_100_end_time) over(partition by crystalid,basearea,BGROUP,stepno6))-unix_timestamp(max(x0_100_start_time) over(partition by crystalid,basearea,BGROUP,stepno6)))/3600 as 0_100_duration,
         |     max(x0_100_start_avgslspeed) over(partition by crystalid,basearea,BGROUP,stepno6)- max(x0_100_end_avgslspeed) over(partition by crystalid,basearea,BGROUP,stepno6) as 0_100_diff_avgslspeed, -- 0_100拉速差值
         |     max(x0_100_end_diameter) over(partition by crystalid,basearea,BGROUP,stepno6)- max(x0_100_start_diameter) over(partition by crystalid,basearea,BGROUP,stepno6) as 0_100_diff_diameter, -- 0-100直径差值
         |     max(x0_100_end_shoulder) over(partition by crystalid,basearea,BGROUP,stepno6)- max(x0_100_start_shoulder) over(partition by crystalid,basearea,BGROUP,stepno6) as 0_100_diff_shoulder, -- 0-100肩形差值
         |     max(x0_100_end_crystallength) over(partition by crystalid,basearea,BGROUP,stepno6)- max(x0_100_start_crystallength) over(partition by crystalid,basearea,BGROUP,stepno6) as 0_100_diff_crystallength, -- 0-100长度差值
         |
         |
         |     (unix_timestamp(max(x100_200_end_time) over(partition by crystalid,basearea,BGROUP,stepno6))-unix_timestamp(max(x100_200_start_time) over(partition by crystalid,basearea,BGROUP,stepno6)))/3600 as 100_200_duration,
         |     max(x100_200_start_avgslspeed) over(partition by crystalid,basearea,BGROUP,stepno6)- max(x100_200_end_avgslspeed) over(partition by crystalid,basearea,BGROUP,stepno6) as 100_200_diff_avgslspeed, -- 100_200拉速差值
         |     max(x100_200_end_diameter) over(partition by crystalid,basearea,BGROUP,stepno6)- max(x100_200_start_diameter) over(partition by crystalid,basearea,BGROUP,stepno6) as 100_200_diff_diameter, -- 100_200直径差值
         |     max(x100_200_end_shoulder) over(partition by crystalid,basearea,BGROUP,stepno6)- max(x100_200_start_shoulder) over(partition by crystalid,basearea,BGROUP,stepno6) as 100_200_diff_shoulder, -- 100_200肩形差值
         |     max(x100_200_end_crystallength) over(partition by crystalid,basearea,BGROUP,stepno6)- max(x100_200_start_crystallength) over(partition by crystalid,basearea,BGROUP,stepno6) as 100_200_diff_crystallength, -- 0-100长度差值
         |
         |     (unix_timestamp(max(x200_300_end_time) over(partition by crystalid,basearea,BGROUP,stepno6))-unix_timestamp(max(x200_300_start_time) over(partition by crystalid,basearea,BGROUP,stepno6)))/3600 as 200_300_duration,
         |     max(x200_300_start_avgslspeed) over(partition by crystalid,basearea,BGROUP,stepno6)- max(x200_300_end_avgslspeed) over(partition by crystalid,basearea,BGROUP,stepno6) as 200_300_diff_avgslspeed, -- 200_300拉速差值
         |     max(x200_300_end_diameter) over(partition by crystalid,basearea,BGROUP,stepno6)- max(x200_300_start_diameter) over(partition by crystalid,basearea,BGROUP,stepno6) as 200_300_diff_diameter, -- 200_300直径差值
         |     max(x200_300_end_shoulder) over(partition by crystalid,basearea,BGROUP,stepno6)- max(x200_300_start_shoulder) over(partition by crystalid,basearea,BGROUP,stepno6) as 200_300_diff_shoulder, -- 200_300肩形差值
         |     max(x200_300_end_crystallength) over(partition by crystalid,basearea,BGROUP,stepno6)- max(x200_300_start_crystallength) over(partition by crystalid,basearea,BGROUP,stepno6) as 200_300_diff_crystallength -- 0-100长度差值
         |
         |from
         |(select *,
         |        case when diameter_group=1 then first_value(nowtime) over(partition by crystalid,basearea,BGROUP,stepno6,diameter_tag order by nowtime)  else null end as x0_100_start_time,--0-100开始时间
         |       case when diameter_group=1 then  first_value(nowtime) over(partition by crystalid,basearea,BGROUP,stepno6,diameter_tag order by nowtime DESC) else null end as x0_100_end_time, -- 0-100结束时间
         |       case when diameter_group=1 then first_value(avgslspeed) over(partition by crystalid,basearea,BGROUP,stepno6,diameter_tag order by nowtime)  else null end as x0_100_start_avgslspeed,--0-100开始拉速
         |       case when diameter_group=1 then  first_value(avgslspeed) over(partition by crystalid,basearea,BGROUP,stepno6,diameter_tag order by nowtime DESC) else null end as x0_100_end_avgslspeed, -- 0-100结束拉速
         |       case when diameter_group=1 then first_value(diameter) over(partition by crystalid,basearea,BGROUP,stepno6,diameter_tag order by nowtime)  else null end as x0_100_start_diameter,--0-100开始直径
         |       case when diameter_group=1 then  first_value(diameter) over(partition by crystalid,basearea,BGROUP,stepno6,diameter_tag order by nowtime DESC) else null end as x0_100_end_diameter, -- 0-100结束直径
         |       case when diameter_group=1 then first_value(shoulder) over(partition by crystalid,basearea,BGROUP,stepno6,diameter_tag order by nowtime)  else null end as x0_100_start_shoulder,--0-100开始肩形
         |       case when diameter_group=1 then  first_value(shoulder) over(partition by crystalid,basearea,BGROUP,stepno6,diameter_tag order by nowtime DESC) else null end as x0_100_end_shoulder, -- 0-100结束肩形
         |       case when diameter_group=1 then first_value(crystallength) over(partition by crystalid,basearea,BGROUP,stepno6,diameter_tag order by nowtime)  else null end as x0_100_start_crystallength,--0-100开始晶体长度
         |       case when diameter_group=1 then  first_value(crystallength) over(partition by crystalid,basearea,BGROUP,stepno6,diameter_tag order by nowtime DESC) else null end as x0_100_end_crystallength, -- 0-100结束晶体长度
         |
         |       case when diameter_group=2 then first_value(nowtime) over(partition by crystalid,basearea,BGROUP,stepno6,diameter_tag order by nowtime)  else null end as x100_200_start_time,--100_200开始时间
         |       case when diameter_group=2 then  first_value(nowtime) over(partition by crystalid,basearea,BGROUP,stepno6,diameter_tag order by nowtime DESC) else null end as x100_200_end_time, -- 100_200结束时间
         |       case when diameter_group=2 then first_value(avgslspeed) over(partition by crystalid,basearea,BGROUP,stepno6,diameter_tag order by nowtime)  else null end as x100_200_start_avgslspeed,--100_200开始拉速
         |       case when diameter_group=2 then  first_value(avgslspeed) over(partition by crystalid,basearea,BGROUP,stepno6,diameter_tag order by nowtime DESC) else null end as x100_200_end_avgslspeed, -- 100_200结束拉速
         |       case when diameter_group=2 then first_value(diameter) over(partition by crystalid,basearea,BGROUP,stepno6,diameter_tag order by nowtime)  else null end as x100_200_start_diameter,--100_200开始直径
         |       case when diameter_group=2 then  first_value(diameter) over(partition by crystalid,basearea,BGROUP,stepno6,diameter_tag order by nowtime DESC) else null end as x100_200_end_diameter, -- 100_200结束直径
         |       case when diameter_group=2 then first_value(shoulder) over(partition by crystalid,basearea,BGROUP,stepno6,diameter_tag order by nowtime)  else null end as x100_200_start_shoulder,--100_200开始肩形
         |       case when diameter_group=2 then  first_value(shoulder) over(partition by crystalid,basearea,BGROUP,stepno6,diameter_tag order by nowtime DESC) else null end as x100_200_end_shoulder, -- 100_200结束肩形
         |       case when diameter_group=2 then first_value(crystallength) over(partition by crystalid,basearea,BGROUP,stepno6,diameter_tag order by nowtime)  else null end as x100_200_start_crystallength,--100_200开始晶体长度
         |       case when diameter_group=2 then  first_value(crystallength) over(partition by crystalid,basearea,BGROUP,stepno6,diameter_tag order by nowtime DESC) else null end as x100_200_end_crystallength, -- 100_200结束晶体长度
         |
         |       case when diameter_group=3 then first_value(nowtime) over(partition by crystalid,basearea,BGROUP,stepno6,diameter_tag order by nowtime)  else null end as x200_300_start_time,--200_300开始时间
         |       case when diameter_group=3 then  first_value(nowtime) over(partition by crystalid,basearea,BGROUP,stepno6,diameter_tag order by nowtime DESC) else null end as x200_300_end_time, -- 200_300结束时间
         |       case when diameter_group=3 then first_value(avgslspeed) over(partition by crystalid,basearea,BGROUP,stepno6,diameter_tag order by nowtime)  else null end as x200_300_start_avgslspeed,--200_300开始拉速
         |       case when diameter_group=3 then  first_value(avgslspeed) over(partition by crystalid,basearea,BGROUP,stepno6,diameter_tag order by nowtime DESC) else null end as x200_300_end_avgslspeed, -- 200_300结束拉速
         |       case when diameter_group=3 then first_value(diameter) over(partition by crystalid,basearea,BGROUP,stepno6,diameter_tag order by nowtime)  else null end as x200_300_start_diameter,--200_300开始直径
         |       case when diameter_group=3 then  first_value(diameter) over(partition by crystalid,basearea,BGROUP,stepno6,diameter_tag order by nowtime DESC) else null end as x200_300_end_diameter, -- 200_300结束直径
         |       case when diameter_group=3 then first_value(shoulder) over(partition by crystalid,basearea,BGROUP,stepno6,diameter_tag order by nowtime)  else null end as x200_300_start_shoulder,--200_300开始肩形
         |       case when diameter_group=3 then  first_value(shoulder) over(partition by crystalid,basearea,BGROUP,stepno6,diameter_tag order by nowtime DESC) else null end as x200_300_end_shoulder, -- 200_300结束肩形
         |       case when diameter_group=3 then first_value(crystallength) over(partition by crystalid,basearea,BGROUP,stepno6,diameter_tag order by nowtime)  else null end as x200_300_start_crystallength,--200_300开始晶体长度
         |       case when diameter_group=3 then  first_value(crystallength) over(partition by crystalid,basearea,BGROUP,stepno6,diameter_tag order by nowtime DESC) else null end as x200_300_end_crystallength -- 200_300结束晶体长度
         |
         |from
         |(select *,
         |sum(xdiameter_tag) over (partition by crystalid,basearea,BGROUP,stepno6 order by nowtime) as diameter_tag --  直径分组
         |from
         |(select *,
         |       round(start_meltsurftemp-start_setmeltsurftemp,2) as  start_diff_surftemp, --放肩开始与目标的亮度偏差
         |       round((unix_timestamp(fj_end_time)-unix_timestamp(fj_start_time))/3600,1) as fj_duration, -- 放肩时长
         |       case when lag_diameter_group is null and  diameter_group=1 then 1 else 0 end as 0_100_start, --0_100 开始
         |       case when diameter_group=1 and lead_diameter_group=2 then 1 else 0 end as 0_100_end,
         |       case when diameter_group=2 and lag_diameter_group=1 then 1 else 0  end as 100_200_start,
         |       case when diameter_group=2 and lead_diameter_group=3 then 1 else 0 end as 100_200_end,
         |       case when diameter_group=3 and lag_diameter_group=2 then 1 else 0 end as 200_300_start,
         |       case when diameter_group=3 and lead_diameter_group is null then 1 else 0 end as 200_300_end,
         |       case when diameter_group != lag_diameter_group then 1 else 0 end  as xdiameter_tag
         |from
         |(select *,
         |       first_value(meltsurftemp) over (partition by crystalid,basearea,BGROUP,stepno6 order by nowtime)   as start_meltsurftemp, --放肩起始亮度
         |       first_value(setmeltsurftemp) over (partition by crystalid,basearea,BGROUP,stepno6 order by nowtime)  as start_setmeltsurftemp, --放肩起始设定亮度
         |       first_value(crystalweight) over (partition by crystalid,basearea,BGROUP,stepno6 order by nowtime ) as fj_start_crystalweight, --放肩起始重量
         |       first_value(crystalweight) over (partition by crystalid,basearea,BGROUP,stepno6 order by nowtime DESC) as fj_end_crystalweight, --放肩结束重量
         |
         |       first_value(crystallength) over (partition by crystalid,basearea,BGROUP,stepno6 order by nowtime ) as fj_start_crystallength, --放肩起始长度
         |       first_value(crystallength) over (partition by crystalid,basearea,BGROUP,stepno6 order by nowtime DESC) as fj_end_crystallength, --放肩结束长度
         |
         |       first_value(nowtime) over (partition by crystalid,basearea,BGROUP,stepno6 order by nowtime) as fj_start_time, --放肩开始时间
         |       first_value(nowtime) over (partition by crystalid,basearea,BGROUP,stepno6 order by nowtime DESC) as fj_end_time, --放肩结束时间
         |
         |       round(avg(avgslspeed) over (partition by crystalid,basearea,BGROUP,stepno6),1) as avg_avgslspeed,--平均拉速
         |       round(avg(diameter) over (partition by crystalid,basearea,BGROUP,stepno6),1) as  avg_diameter,--平均直径
         |       round(avg(cruciblelift) over (partition by crystalid,basearea,BGROUP,stepno6),1) as avg_cruciblelift,--平均埚升
         |       round(avg(meltlevel) over (partition by crystalid,basearea,BGROUP,stepno6),1) as avg_meltlevel,--平均液口距
         |       round(avg(cruciblepos) over (partition by crystalid,basearea,BGROUP,stepno6),1) as avg_cruciblepos,--平均埚位
         |       case when crystallength=0 then 0 else xshoulder end as shoulder,
         |       lag(diameter_group) over (partition by crystalid,basearea,BGROUP,stepno6 order by nowtime) as lag_diameter_group,
         |       lead(diameter_group) over (partition by crystalid,basearea,BGROUP,stepno6 order by nowtime) as lead_diameter_group
         |       from
         |(select *,
         |       lag(stepno6) over(partition by crystalid,basearea,BGROUP order by nowtime) as lag_stepno6,--上次放肩分组
         |       round(diameter/crystallength,4) as  xshoulder, --肩形
         |       case when diameter>=0 and diameter<100 then 1 else 0 end as 0_100_group,
         |       case when diameter>=100 and diameter<=200 then 1 else 0 end as 100_200_group,
         |
         |       case  when diameter>200 then 1 else 0  end as  200_300_group,
         |       case  when diameter>=0 and diameter<100 then 1
         |             when diameter>=100 and diameter<=200 then 2
         |             when diameter>200 then 3 end as diameter_group
         |       from bggroupSqlDF_view
         |)ML)NL)OL
         |         where (
         |        0_100_start=1
         |        or 0_100_end=1
         |        or 100_200_start=1
         |        or 100_200_end=1
         |        or 200_300_start=1
         |        or 200_300_end=1)
         |    )PL)RL)SL)TL
         |where rank_num=1
         |""".stripMargin

    val fjSqlDF=spark.sql(fjSql)
    fjSqlDF.createTempView("fjSqlDF_view")

    // step3 熔料

    val rlSql =
      """
        |select
        |        basearea,
        |        crystalid,
        |        nowtime,
        |       state,
        |       remainweight,                                              --剩料量
        |       base,                                                      --基地
        |       puller,                                                    --炉号
        |       round(end_remainweight / rl_duration, 4) as rl_efficiency, -- 熔料效率
        |       end_remainweight,                                          -- 装埚料量
        |       rl_duration,                                                -- 熔料时长小时
        |       start_add_material_time,  -- 熔料开始时间
        |       end_add_material_time -- 熔料结束时间
        | from
        |(select *,
        |   max(xend_remainweight) OVER (PARTITION BY crystalid, BASEAREA) as end_remainweight, -- 装埚料量
        |   round((unix_timestamp(end_add_material_time) - unix_timestamp(start_add_material_time)) / 3600,1) as    rl_duration -- 熔料时长hour
        |from
        |(select *,
        |min(nowtime) over (partition by crystalid,BASEAREA) as start_add_material_time,  -- 熔料开始时间
        |max(nowtime) over (partition by crystalid,BASEAREA) as end_add_material_time, -- 熔料结束时间
        |CASE WHEN ROW_NUMBER() OVER (PARTITION BY crystalid, BASEAREA ORDER BY nowtime DESC) = 1 THEN remainweight END AS xend_remainweight
        |from
        |(select
        |*,
        |count(*) over (partition by crystalid,BASEAREA) as num
        |from
        |( select * ,
        |case when state =3 and last_state = 2 then 1 else 0 end as is_start_jl, --加料开始
        |case when state =3 and next_state = 4 then 1 else 0 end as is_end_jl --加料结束
        |from
        |(SELECT basearea,
        |                         crystalid,
        |                         nowtime,
        |                         state,
        |                         remainweight,                    --剩料量
        |                         substr(basearea, 1, 2) as base,  --基地
        |                         SUBSTR(BASEAREA, -3)   as puller, --炉号
        |                         lag(state, 1) over (partition by crystalid,BASEAREA order by nowtime) last_state, --上一个工步
        |                         lead(state, 1) over (partition by crystalid,BASEAREA order by nowtime) next_state --下一个工步
        |                  FROM ods_ingot_scada.djzk_app_result
        |                  WHERE
        |                        day_id >= '2023-01-01'
        |                     and day_id <= '2023-08-10'
        |                    and state in(2,3,4)
        |                    and crystalid !='DS'
        |                    and length(crystalid)>=10
        |                     and substr(basearea, 1, 2)='QJ'
        |--                    and BASEAREA = 'GF_B96'
        |                --    and crystalid = 'BBS38B5401'
        |    )AL)BL
        |where state=3
        |and (is_start_jl=1 or is_end_jl=1))CL
        |where num=2)DL)EL
        |where is_start_jl=1
        |""".stripMargin


    val rlSqlDF = spark.sql(rlSql)

    val resultDF =   fjSqlDF.join(rlSqlDF,fjSqlDF.col("crystalid") ===rlSqlDF.col("crystalid"))
      .select(fjSqlDF.col("*"),
        rlSqlDF.col("rl_efficiency"),
        rlSqlDF.col("end_remainweight"),
        rlSqlDF.col("rl_duration"),
        rlSqlDF.col("start_add_material_time"),
        rlSqlDF.col("end_add_material_time")
      ).withColumn("day_id", substring(col("nowtime"), 1, 11))


    resultDF.show(20)
    resultDF.printSchema()

    resultDF.createTempView("resultDF_view")
    spark.sql(
      """
        |INSERT OVERWRITE TABLE test.dj_data_analyse partition(day_id)
        |SELECT * FROM resultDF_view
        |""".stripMargin)
    spark.stop()


  }
}
