package org.example.tasks

import java.util.Properties

object AppConfig {
  private val props: Properties = new Properties()
  try {
    val is = this.getClass.getClassLoader.getResourceAsStream("config.properties")
    if (is != null) props.load(is)
  } catch {
    case _: Exception => // ignore, will use defaults
  }

  def getString(key: String, default: String): String =
    Option(props.getProperty(key)).getOrElse(default)

  def getInt(key: String, default: Int): Int =
    try Option(props.getProperty(key)).map(_.toInt).getOrElse(default)
    catch { case _: Exception => default }

  def getDouble(key: String, default: Double): Double =
    try Option(props.getProperty(key)).map(_.toDouble).getOrElse(default)
    catch { case _: Exception => default }

  def getBoolean(key: String, default: Boolean): Boolean =
    try Option(props.getProperty(key)).map(_.toBoolean).getOrElse(default)
    catch { case _: Exception => default }
}
