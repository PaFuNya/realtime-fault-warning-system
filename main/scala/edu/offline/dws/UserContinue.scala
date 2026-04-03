package edu.offline.dws

import org.apache.spark.sql.SparkSession

object UserContinue {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    //1、设置sparksession
    val ss:SparkSession=SparkSession.builder()
      .master("local[*]")
      .appName("Hudi avg province")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()

    ss.read.format("hudi")
      .load("hdfs://master:8020/user/hive/warehouse/dwd_ds_hudi.db/fact_order_info")
      .createTempView("order_v")
    ss.read.format("hudi")
      .load("hdfs://master:8020/user/hive/warehouse/dwd_ds_hudi.db/dim_user_info")
      .createTempView("user_v")

    ss.sql(
      """
        |select o.user_id as userid,u.login_name as username,
        |to_date(o.create_time) as create_date,
        |sum(cast(final_total_amount as double)) as amount,
        |count(final_total_amount) as count_order
        |from
        |order_v o join user_v u on o.user_id=u.id
        |group by userid,username,create_date
        |""".stripMargin
    ).createTempView("sum_v")


    val df=ss.sql(
      """
        |select userid,username,
        |concat(date_format(predate,"yyyyMMdd"),"_",date_format(create_date,"yyyyMMdd")) as day,
        |amount,create_date,
        |preamount,predate,
        |amount+preamount  totalconsumption,
        |count_order+precount totalorder
        |from
        |(select userid,username,create_date,amount,count_order,
        |lag(create_date,1,"1970-01-01") over (partition by userid order by create_date) predate,
        |lag(amount,1,"0.0") over (partition by userid order by create_date) preamount,
        |lag(count_order,1,"0") over (partition by userid order by create_date) precount
        |from sum_v) v
        |where datediff(create_date,predate)=1
        |and amount>preamount
        |""".stripMargin
    )

    /*val df=ss.sql(
      """
        |select userid,username,
        |concat(date_format(predate,"yyyyMMdd"),"_",date_format(create_date,"yyyyMMdd")),
        |amount+preamount as totalconsumption,
        |count(userid) over(partition by userid) totalorder
        |from
        |(select
        |o.user_id as userid,
        |u.login_name as username,
        |to_date(o.create_time) as create_date,
        |cast(o.final_total_amount as double) as amount,
        |lag(to_date(o.create_time),1,"1970-01-01") over (partition by o.user_id order by to_date(o.create_time)) predate,
        |lag(cast(o.final_total_amount as double),1,"0.0") over (partition by o.user_id order by to_date(o.create_time)) preamount
        |from
        |order_v o join user_v u
        |on o.user_id=u.id)
        |where datediff(create_date,predate)=1
        |and amount>preamount
        |""".stripMargin
    )*/
    df.show()
    ss.close()
  }

}
