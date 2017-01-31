package com.qunar.spark.base.io

import javax.annotation.Resource

import com.google.common.base.{Preconditions, Strings}
import com.hadoop.compression.lzo.LzopCodec
import com.hadoop.mapreduce.LzoTextInputFormat
import com.qunar.spark.base.io.HdfsFileType.HdfsFileType
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.springframework.stereotype.Service

/**
  * Hdfs读写简单封装
  */
@Service
class HdfsService extends Serializable {

  @Resource
  private var context: SparkContext = _

  /* 读Hdfs的封装 */

  def readFromHdfs(path: String, hdfsFileType: HdfsFileType): RDD[String] = {
    Preconditions.checkArgument(Strings.isNullOrEmpty(path), "hdfs path is null": Any)
    Preconditions.checkNotNull(hdfsFileType)

    hdfsFileType match {
      case HdfsFileType.TEXT => readFromText(path)
      case HdfsFileType.GZIP => readFromText(path)
      case HdfsFileType.LZO => readFromLzo(path)
    }
  }

  private def readFromLzo(path: String): RDD[String] = {
    context.newAPIHadoopFile[LongWritable, Text, LzoTextInputFormat](path).map(e => e._2.toString)
  }

  private def readFromText(path: String): RDD[String] = {
    context.textFile(path)
  }

  /* 写Hdfs的封装 */

  def writeEntityToHdfs[T](content: RDD[T], serialize: T => String, path: String, hdfsFileType: HdfsFileType)
                          (partition: Int = 1000): Unit = {
    val result = content.map(e => serialize(e))
    writeStrToHdfs(result, path, hdfsFileType)(partition)
  }

  def writeStrToHdfs(content: RDD[String], path: String, hdfsFileType: HdfsFileType)
                    (partition: Int = 1000): Unit = {
    val cleanContent = content.filter(StringUtils.isNotBlank)
    val adjustContent = partitionAdjustment(cleanContent, partition)
    Preconditions.checkNotNull(path)
    hdfsFileType match {
      case HdfsFileType.TEXT => writeAsText(adjustContent, path)
      case HdfsFileType.GZIP => writeAsText(adjustContent, path)
      case HdfsFileType.LZO => writeAsLzo(adjustContent, path)
    }
  }

  /**
    * 分区调整
    */
  private def partitionAdjustment(content: RDD[String], partition: Int): RDD[String] = {
    //分区前后缩放比例
    val rate = partition / content.partitions.length
    /*
    * 如果分区缩小且缩小比例小于1个数量级,则可以不用shuffle
    * 否则(包括分区扩大)需要shuffle
    */
    rate match {
      case r if r > 0.1 || r < 1 => content.coalesce(partition, shuffle = false)
      case _ => content.repartition(partition)
    }
  }

  private def writeAsText(content: RDD[String], path: String): Unit = {
    content.saveAsTextFile(path)
  }

  private def writeAsLzo(content: RDD[String], path: String): Unit = {
    content.saveAsTextFile(path, classOf[LzopCodec])
  }

}

object HdfsFileType extends Enumeration {

  type HdfsFileType = Value

  /**
    * Hdfs的文件类型(普通文本, lzo, gz)
    */
  val TEXT, LZO, GZIP = Value

}
