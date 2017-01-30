package com.qunar.spark.base.json

import java.io.{Reader, Writer}

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.google.common.base.Preconditions

/**
  * 封装Jackson的ObjectMapper
  * copied from qunar.common.api by @miao.yang and customize it
  *
  * 相对common-api原版本的改动:
  * 1. JsonMapper增加readTree与JsonNode的支持
  * 2. JsonMapper变成静态Object,且只使用默认JsonFeature配置,方便外部直接调用而免去注入的过程,
  * 但同时保留定制化的接口以及工厂注入的功能
  */
private[jackson] object JsonMapper extends Serializable {

  // 默认JsonFeature模板的ObjectMapperBuilder
  private val mapper: ObjectMapper = MapperBuilder.create.buildDefault

  def writeValue(writer: Writer, obj: Any) {
    Preconditions.checkNotNull(writer)
    mapper.writeValue(writer, obj)
  }

  def writeValueAsString(obj: Any): String = mapper.writeValueAsString(obj)

  def readValue[T](json: String, clazz: Class[T]): T = {
    mapper.readValue(json, clazz)
  }

  def readValue[T](reader: Reader, clazz: Class[T]): T = {
    Preconditions.checkNotNull(reader)
    mapper.readValue(reader, clazz)
  }

  def readValue[T](json: String, givenType: TypeReference[T]): T = {
    mapper.readValue(json, givenType)
  }

  def readValue[T](reader: Reader, givenType: TypeReference[T]): T = {
    Preconditions.checkNotNull(reader)
    mapper.readValue(reader, givenType)
  }

  /* readTree 系列 */

  def readTree(json: String): JsonNode = {
    mapper.readTree(json)
  }

  def getNodeStr(jsonNode: JsonNode, name: String): String = {
    jsonNode.get(name).asText
  }

  def getNodeInt(jsonNode: JsonNode, name: String): Int = {
    jsonNode.get(name).asInt
  }

  def getNodeLong(jsonNode: JsonNode, name: String): Long = {
    jsonNode.get(name).asLong
  }

  def getNodeDouble(jsonNode: JsonNode, name: String): Double = {
    jsonNode.get(name).asDouble
  }

}
