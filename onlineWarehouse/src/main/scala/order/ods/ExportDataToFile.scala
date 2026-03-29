package order.ods

import java.sql.{Connection, DriverManager, ResultSet}
import java.io.PrintWriter
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ListBuffer
import java.sql.Timestamp
import scala.math.Ordering.Implicits._

object ExportDataToFile {
  def main(args: Array[String]): Unit = {
    // 数据库配置
    val jdbcUrl = "jdbc:mysql://192.168.45.16:3306/ocean"
    val dbUser = "root"
    val dbPassword = "123456"

    // 输出文件路径
    val outputPath = "ocean_data.txt"

    // 查询语句
    val barometricQuery = "SELECT * FROM barometric_pressure_data"
    val waterLevelQuery = "SELECT   stationID, stationName, longitude, latitude,STR_TO_DATE(eventTime, '%Y-%m-%dT%H:%i:%sZ') AS eventTime, datum, waterLevel FROM water_level_data"
    val weatherQuery = "SELECT * FROM weather_data"

    Class.forName("com.mysql.cj.jdbc.Driver")
    // JDBC 连接
    var connection: Connection = null



    try {
      // 建立连接
//      connection = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword)
      val connectionUrl = "jdbc:mysql://192.168.45.16:3306/ocean?serverTimezone=UTC"
      val connection = DriverManager.getConnection(connectionUrl, "root", "123456")
      // 创建输出文件
      val writer = new PrintWriter(outputPath)
      // 定义一个统一的记录类
      case class Record(identifier: String, timestamp: java.sql.Timestamp, data: String)

      // 存储所有记录的缓冲区
      val allRecords = ListBuffer[Record]()
      // 写入气压数据
      writer.println("B,\"stationID\",\"stationName\",\"longitude\",\"latitude\",\"Time\",\"sensor\",\"BP\"")
      val barometricResultSet = connection.createStatement().executeQuery(barometricQuery)

      while (barometricResultSet.next()) {
        val timestamp = barometricResultSet.getTimestamp("Time")
        val rowData = Seq(
          "B",
          barometricResultSet.getString("stationID"),
          barometricResultSet.getString("stationName"),
          barometricResultSet.getBigDecimal("longitude").toString,
          barometricResultSet.getBigDecimal("latitude").toString,
          Option(timestamp).map(_.toString).getOrElse(),
//          barometricResultSet.getTimestamp("Time").toString,
          barometricResultSet.getString("sensor"),
          barometricResultSet.getBigDecimal("BP").toString
        ).mkString(",")
        allRecords += Record("B", timestamp, rowData)
//        writer.println(rowData)
      }

      // 写入水位数据
      writer.println("W,\"stationID\",\"stationName\",\"longitude\",\"latitude\",\"eventTime\",\"datum\",\"waterLevel\"")
      val waterLevelResultSet = connection.createStatement().executeQuery(waterLevelQuery)
      while (waterLevelResultSet.next()) {
        val timestamp = waterLevelResultSet.getTimestamp("eventTime")
        val rowData = Seq(
          "W",
          Option(waterLevelResultSet.getString("stationID")).getOrElse(""),
          Option(waterLevelResultSet.getString("stationName")).getOrElse(""),
          Option(waterLevelResultSet.getBigDecimal("longitude")).map(_.toString).getOrElse(""),
          Option(waterLevelResultSet.getBigDecimal("latitude")).map(_.toString).getOrElse(""),
          Option(timestamp).map(_.toString).getOrElse(),
          //Option(waterLevelResultSet.getTimestamp("eventTime")).map(_.toString).getOrElse(""),
          Option(waterLevelResultSet.getString("datum")).getOrElse(""),
          Option(waterLevelResultSet.getBigDecimal("waterLevel")).map(_.toString).getOrElse("")
        ).mkString(",")
        allRecords += Record("W", timestamp, rowData)
//        writer.println(rowData)
      }

      // 写入气象数据
      writer.println("M,\"stationID\",\"stationName\",\"longitude\",\"latitude\",\"time\",\"sensor\",\"WS\",\"WD\",\"WG\"")
      val weatherResultSet = connection.createStatement().executeQuery(weatherQuery)
      while (weatherResultSet.next()) {
        val timestamp = weatherResultSet.getTimestamp("time")
        // 类似处理 weatherResultSet 的字段
        val rowData = Seq(
          "M",
          Option(weatherResultSet.getString("stationID")).getOrElse(""),
          Option(weatherResultSet.getString("stationName")).getOrElse(""),
          Option(weatherResultSet.getBigDecimal("longitude")).map(_.toString).getOrElse(""),
          Option(weatherResultSet.getBigDecimal("latitude")).map(_.toString).getOrElse(""),
          Option(timestamp).map(_.toString).getOrElse(),
//          Option(weatherResultSet.getTimestamp("time")).map(_.toString).getOrElse(""),
          Option(weatherResultSet.getString("sensor")).getOrElse(""),
          Option(weatherResultSet.getBigDecimal("WS")).map(_.toString).getOrElse(""),
          Option(weatherResultSet.getBigDecimal("WD")).map(_.toString).getOrElse(""),
          Option(weatherResultSet.getBigDecimal("WG")).map(_.toString).getOrElse("")
        ).mkString(",")
        allRecords += Record("M", timestamp, rowData)
//        writer.println(rowData)
      }
      // 按时间戳排序记录
      implicit val timestampOrdering: Ordering[Timestamp] = Ordering.by(_.getTime)
      val sortedRecords = allRecords.sortBy(_.timestamp)
      // 将排序后的记录写入文件
      sortedRecords.foreach(record => writer.println(record.data))

      // 关闭文件
      writer.close()
      println(s"数据已成功导出到文件: $outputPath")

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null) connection.close()
    }
  }
}
