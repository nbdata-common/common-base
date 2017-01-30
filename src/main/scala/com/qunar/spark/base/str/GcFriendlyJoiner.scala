package com.qunar.spark.base.str

import com.google.common.base.Preconditions
import org.apache.commons.lang3.StringUtils

/**
  * 预估内存开销的字符串连接器
  */
class GcFriendlyJoiner private (separator: String) {

  def join[A](elems: A*): String = {
    joinInternal(elems.map(e => e.toString))
  }

  private def joinInternal(strs: Seq[String]): String = {
    strs.filter(StringUtils.isNotBlank)

    val length = computeStringBuilderCapacity(strs, separator)
    val joiner: StringBuilder = new StringBuilder(length)

    var first = true

    for (str <- strs) {
      if (first) {
        joiner append str
        first = false
      }
      else {
        joiner append separator
        joiner append str
      }
    }

    joiner.toString()
  }

  /**
    * 为StringBuilder计算一个合适的长度
    */
  private def computeStringBuilderCapacity(strs: Seq[String], separator: String): Int = {
    // 待连接字符串集的总长度
    var mainLength = 0
    strs.foreach(str => mainLength += str.length)
    // 分隔符的总长度
    val separatorLength = (strs.length - 1) * separator.length

    mainLength + separatorLength
  }

}

object GcFriendlyJoiner {

  def on(separator: String): GcFriendlyJoiner = {
    Preconditions.checkNotNull[String](separator)
    val joiner = new GcFriendlyJoiner(separator)

    joiner
  }

}
