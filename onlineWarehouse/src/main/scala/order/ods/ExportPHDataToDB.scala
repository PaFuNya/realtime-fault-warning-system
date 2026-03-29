package order.ods

import java.sql.{Connection, DriverManager, PreparedStatement}
import scala.util.Random
import java.math.{BigDecimal => JavaBigDecimal, RoundingMode}
object ExportPHDataToDB {
  def main(args: Array[String]): Unit = {
    // 数据库配置
    val jdbcUrl = "jdbc:mysql://124.71.173.233:3306/ocean"
    val dbUser = "root"
    val dbPassword = "a1293771ac724cea"

    // 定义常量值
    val stationName = "eastSea"
    val sensor = "PH"
    val measurementTime = "2025-03-21 12:00:00"

    // 经度和纬度的新范围
    val longitudeMin = 122.6
    val longitudeMax = 123.5
    val latitudeMin = 27.45
    val latitudeMax = 30.45

    // 随机数生成器用于生成不同的longitude, latitude和DO值
    val random = new Random()

    // JDBC 连接
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null

    try {
      // 建立连接
      connection = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword)

      // 2. 关闭 autocommit
      if (connection != null && !connection.isClosed) {
        connection.setAutoCommit(false) // 关闭自动提交
      }
      // 准备插入语句
      val insertSQL = """
        INSERT INTO ph_data
          (stationID, stationName, longitude, latitude, measurementTime, sensor, pH)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      """
      preparedStatement = connection.prepareStatement(insertSQL)
      // 生成并插入2000条数据
      for (stationID <- 1 to 2000) {
        // 生成随机的longitude, latitude和pH值
        val longitude = longitudeMin + (longitudeMax - longitudeMin) * random.nextDouble() // 经度范围：122.6 ~ 123.5
        val latitude = latitudeMin + (latitudeMax - latitudeMin) * random.nextDouble()     // 纬度范围：27.45 ~ 30.45
        val pHValue = 7.0 + random.nextDouble() * 2                                         // pH值范围：7.0 ~ 9.0


        // 设置PreparedStatement参数
        preparedStatement.setInt(1, stationID)
        preparedStatement.setString(2, stationName)
        preparedStatement.setBigDecimal(3, new JavaBigDecimal(longitude).setScale(6, RoundingMode.HALF_UP))
        preparedStatement.setBigDecimal(4, new JavaBigDecimal(latitude).setScale(6, RoundingMode.HALF_UP))
        preparedStatement.setString(5, measurementTime)
        preparedStatement.setString(6, sensor)
        preparedStatement.setBigDecimal(7, new JavaBigDecimal(pHValue).setScale(2, RoundingMode.HALF_UP))


        // 执行插入
        preparedStatement.addBatch()

        // 每100条记录提交一次批次操作
        if (stationID % 100 == 0) {
          preparedStatement.executeBatch()
          println(s"Inserted $stationID records...")
        }
      }

      // 提交剩余的批次操作
      preparedStatement.executeBatch()

      // 提交事务
      connection.commit()
      println("所有数据已成功插入数据库！")

    } catch {
      case e: Exception => e.printStackTrace()
        if (connection != null && !connection.isClosed) {
          connection.rollback() // 发生异常时回滚事务
        }
    } finally {
      // 关闭资源
      if (preparedStatement != null) preparedStatement.close()
      if (connection != null) connection.close()
    }
  }
}
