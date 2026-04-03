package org.example.tasks

import java.sql.DriverManager

object ClearCK {
  def main(args: Array[String]): Unit = {
    val url = SparkUtils.ckUrl
    val user = "default"
    val password = "password" // Or 123456

    // Try connecting and truncating
    try {
      Class.forName("ru.yandex.clickhouse.ClickHouseDriver")
      var conn = DriverManager.getConnection(url, "default", "")

      val stmt = conn.createStatement()
      val tables = Seq(
        "device_realtime_status",
        "realtime_alerts",
        "realtime_process_deviation",
        "realtime_status_window"
      )

      tables.foreach { table =>
        try {
          stmt.executeUpdate(s"TRUNCATE TABLE IF EXISTS $table")
          println(s"Successfully truncated table: $table")
        } catch {
          case e: Exception =>
            println(s"Failed to truncate $table: ${e.getMessage}")
        }
      }

      stmt.close()
      conn.close()
    } catch {
      case e: Exception =>
        println(s"Failed to connect to ClickHouse: ${e.getMessage}")
    }
  }
}
