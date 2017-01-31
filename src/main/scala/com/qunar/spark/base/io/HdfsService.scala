package com.qunar.spark.base.io

import java.io.{BufferedReader, IOException, InputStream, InputStreamReader}
import java.util
import java.util.zip.GZIPInputStream

import com.google.common.collect.Lists
import com.qunar.spark.base.log.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

/**
  * 针对[[org.apache.hadoop.fs.FileSystem]]的hdfs读写简单封装
  */
object HdfsService extends Serializable with Logging {

  private val conf = new Configuration()

  private val fs = FileSystem.get(conf)

  def listDirectory(path: Path): util.ArrayList[Path] = {
    val fileStatuses: Array[FileStatus] = fs.listStatus(path)
    val ret = Lists.newArrayList[Path]()
    for (fileStatus <- fileStatuses) {
      ret.add(fileStatus.getPath)
    }
    ret
  }

  def isDirectory(path: Path): Boolean = {
    try fs.isDirectory(path)
    catch {
      case e: IOException =>
        logError(s"error during isDirectory:$path", e)
        false
    }
  }

  /**
    * zcat the given file, extract the first limit lines.
    *
    * @param path  The file path of the gzip file.
    * @param limit The number of lines to extract. 0 means all lines.
    * @return The first limit lines of the given file.
    */
  def zcatPipeHead(path: Path, limit: Int): util.ArrayList[String] = {
    var reader: BufferedReader = null
    val ret = Lists.newArrayList[String]()
    try {
      reader = new BufferedReader(
        new InputStreamReader(new GZIPInputStream(fs.open(path))))
      if (limit == 0) {
        var line: String = reader.readLine()
        while (line != null) {
          logInfo(s"read line: $line")
          line = reader.readLine()
        }
        ret.add("check log")
      } else {
        for (i <- 0 until limit) {
          val line: String = reader.readLine()
          if (line == null) {
            //break
          }
          ret.add(line)
        }
      }
    } catch {
      case e: IOException => logError("zcatPipeHead failed: {}", e)
    } finally try if (reader != null) reader.close()
    catch {
      case e: IOException => logError("close reader failed: {}", e)
    }
    ret
  }

  def catPipeHead(path: Path, limit: Int): util.ArrayList[String] = {
    var reader: BufferedReader = null
    val ret = Lists.newArrayList[String]()
    try {
      reader = new BufferedReader(new InputStreamReader(fs.open(path)))
      if (limit == 0) {
        var line: String = reader.readLine()
        while (line != null) {
          ret.add(line)
          //logger.info("read line: {}", line);
          line = reader.readLine()
        }
      } else {
        for (i <- 0 until limit) {
          val line: String = reader.readLine()
          if (line == null) {
            //break
          }
          ret.add(line)
        }
      }
    } catch {
      case e: Exception => logError("catPipeHead failed:", e)
    } finally try if (reader != null) reader.close()
    catch {
      case e: Exception => logError("close reader failed: {}", e)
    }
    ret
  }

  /**
    * Opens an HDFS file as InputStream.
    *
    * @param path Path to the file.
    * @return The InputStream for the given file.
    * @throws IOException Opening the HDFS file can throw IOException.
    */
  def open(path: Path): InputStream = {
    if (path.getName.endsWith(".gz")) {
      new GZIPInputStream(fs.open(path))
    } else {
      fs.open(path)
    }
  }

  def create(path: Path): FSDataOutputStream = fs.create(path)

  /**
    * 将内容写进给定的path
    */
  def writeContentToPath(path: Path, content: String) = {
    var outputStream: FSDataOutputStream = null
    if (!fs.exists(path))
      outputStream = fs.create(path)
    else
      outputStream = fs.append(path)
    try outputStream.write(content.getBytes())
    catch {
      case e: IOException => logError(s"write content = $content to path = $path error:", e)
    }
  }

  /**
    * hadoop fs -cp <src> <dst>
    *
    * @return if action is successful
    */
  def copy(src: Path, dst: Path): Boolean = {
    try FileUtil.copy(fs, src, fs, dst, false, new Configuration())
    catch {
      case e: IOException =>
        logError(s"copy from $src to $dst failed:", e)
        false
    }
  }

  def delete(path: Path, recursive: Boolean): Boolean = {
    try fs.delete(path, recursive)
    catch {
      case e: IOException =>
        logError(s"delete file failed:$path", e)
        false
    }
  }

  def close() = fs.close()

}
