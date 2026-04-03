package com.shtd.util

import java.util.Properties
import java.io.{FileInputStream, InputStream}

object ConfigUtil {

  // 缓存已加载的配置，避免重复加载
  private var cachedProps: Properties = _

  /**
   * 从 classpath 加载配置文件
   * @param configPath 配置文件路径（相对于 classpath）
   * @return Properties 对象
   */
  def loadProperties(configPath: String = "config.properties"): Properties = {
    if (cachedProps != null) {
      return cachedProps
    }

    val props = new Properties()
    val inputStream: InputStream = getClass.getClassLoader.getResourceAsStream(configPath)

    if (inputStream == null) {
      throw new RuntimeException(s"错误：无法找到配置文件 $configPath")
    }

    try {
      props.load(inputStream)
      cachedProps = props
      println(s">>> 成功加载配置文件：$configPath")
    } finally {
      inputStream.close()
    }

    props
  }

  /**
   * 从指定文件路径加载配置文件
   * @param filePath 文件绝对路径或相对路径
   * @return Properties 对象
   */
  def loadPropertiesFromFile(filePath: String): Properties = {
    val props = new Properties()
    val inputStream = new FileInputStream(filePath)

    try {
      props.load(inputStream)
      println(s">>> 成功加载配置文件：$filePath")
    } finally {
      inputStream.close()
    }

    props
  }

  /**
   * 获取字符串配置值
   * @param key 配置键
   * @param defaultValue 默认值
   * @return 配置值
   */
  def getString(key: String, defaultValue: String = ""): String = {
    if (cachedProps == null) loadProperties()
    cachedProps.getProperty(key, defaultValue)
  }

  /**
   * 获取整数配置值
   * @param key 配置键
   * @param defaultValue 默认值
   * @return 配置值
   */
  def getInt(key: String, defaultValue: Int = 0): Int = {
    if (cachedProps == null) loadProperties()
    cachedProps.getProperty(key, defaultValue.toString).toInt
  }

  /**
   * 获取布尔配置值
   * @param key 配置键
   * @param defaultValue 默认值
   * @return 配置值
   */
  def getBoolean(key: String, defaultValue: Boolean = false): Boolean = {
    if (cachedProps == null) loadProperties()
    cachedProps.getProperty(key, defaultValue.toString).toBoolean
  }

  /**
   * 获取长整型配置值
   * @param key 配置键
   * @param defaultValue 默认值
   * @return 配置值
   */
  def getLong(key: String, defaultValue: Long = 0L): Long = {
    if (cachedProps == null) loadProperties()
    cachedProps.getProperty(key, defaultValue.toString).toLong
  }

  /**
   * 清除缓存的配置（用于重新加载）
   */
  def clearCache(): Unit = {
    cachedProps = null
  }
}
