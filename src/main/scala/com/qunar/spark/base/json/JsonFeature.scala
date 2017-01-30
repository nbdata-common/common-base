package com.qunar.spark.base.json

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind._

import scala.beans.BeanProperty
import scala.language.implicitConversions

/**
  * 用于标记可编辑的json特性
  * copied from qunar.common.api by @miao.yang and customize it
  */
private[jackson] object JsonFeature extends Enumeration {

  type JsonFeature = Value

  val FAIL_ON_UNKNOWN_PROPERTIES = new JsonFeatureValue(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  val AUTO_CLOSE_TARGET = new JsonFeatureValue(JsonGenerator.Feature.AUTO_CLOSE_TARGET, true)

  val INDENT_OUTPUT = new JsonFeatureValue(SerializationFeature.INDENT_OUTPUT, false)
  val WRITE_DATES_AS_TIMESTAMPS = new JsonFeatureValue(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true)
  val WRITE_NULL_MAP_VALUES = new JsonFeatureValue(SerializationFeature.WRITE_NULL_MAP_VALUES, false)
  val FAIL_ON_EMPTY_BEANS = new JsonFeatureValue(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)

  val INCLUSION_NOT_NULL = new JsonFeatureValue(JsonInclude.Include.NON_NULL, false)
  val INCLUSION_NOT_EMPTY = new JsonFeatureValue(JsonInclude.Include.NON_EMPTY, false)

  val ALLOW_COMMENTS = new JsonFeatureValue(JsonParser.Feature.ALLOW_COMMENTS, true)
  val ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER = new JsonFeatureValue(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, true)
  val ALLOW_NON_NUMERIC_NUMBERS = new JsonFeatureValue(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true)
  val ALLOW_NUMERIC_LEADING_ZEROS = new JsonFeatureValue(JsonParser.Feature.ALLOW_NUMERIC_LEADING_ZEROS, true)
  val ALLOW_UNQUOTED_CONTROL_CHARS = new JsonFeatureValue(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true)
  val ALLOW_UNQUOTED_FIELD_NAMES = new JsonFeatureValue(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
  val ALLOW_SINGLE_QUOTES = new JsonFeatureValue(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)

  // 默认的JsonFeature配置模板
  val defaults: Long = {
    var flags = 0
    for (f <- values if f.enabledByDefault) {
      flags |= f.getMask
    }

    flags
  }

  sealed case class JsonFeatureValue (@BeanProperty feature: AnyRef, enabledByDefault: Boolean) extends Val {

    @BeanProperty val mask = 1 << id

    def isEnabled(flags: Long): Boolean = (flags & mask) != 0

    def enable(flags: Long): Long = flags | mask

    def disable(flags: Long): Long = flags & (~mask)

  }

  implicit def convertValue(v: Value): JsonFeatureValue = v.asInstanceOf[JsonFeatureValue]

}
