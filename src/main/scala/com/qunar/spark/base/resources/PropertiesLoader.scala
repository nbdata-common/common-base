package com.qunar.spark.base.resources

import com.typesafe.config.{Config, ConfigFactory}

/**
  * 用于spark任务读取本地的properties文件
  */
class PropertiesLoader(private val resourceName: String) extends Serializable {

  private val conf: Config = ConfigFactory.load(resourceName)

  def getStr(key: String) = {
    conf.getString(key)
  }

  def getStrOrDefault(key: String, default: String) = {
    try {
      getStr(key)
    } catch {
      case e: Exception => default
    }
  }

  def getInt(key: String) = {
    conf.getInt(key)
  }

  def getIntOrDefault(key: String, default: Int) = {
    try {
      getInt(key)
    } catch {
      case e: Exception => default
    }
  }

}

object PropertiesLoader {

  def apply(resourceName: String): PropertiesLoader = new PropertiesLoader(resourceName)

}
