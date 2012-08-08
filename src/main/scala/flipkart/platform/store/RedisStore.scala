package flipkart.platform.store

import scala.collection.JavaConversions._
import redis.clients.jedis._
import flipkart.platform.file.{FileStatus, FileFields, FileMetaData}
import java.io.IOException
import collection.mutable.HashMap

/**
 * Created by IntelliJ IDEA.
 * User: vivekys
 * Date: 02/08/12
 * Time: 1:42 PM
 * To change this template use File | Settings | File Templates.
 */

class RedisStore(val host: String, val port: Int) extends MetaStore
{

  val redisHandler = new Jedis(host, port)

  val FILEMAP = "FILEMAP"

  val FILEDIR = "FILEDIR"

  val FILECHUNK = "FILECHUNK"

  log.info("Creating RedisStore with = " + host + ":" + port)

  private def schemeFileNameForDir(fileName: String) = FILEMAP + fileName

  private def schemeFileNameForChunks(fileName: String) = FILECHUNK + fileName

  def createFile(fileName: String, attr: FileMetaData): Boolean =
  {
    if (redisHandler.hexists(FILEDIR, fileName))
    {
      log.debug("Creation of file :" + fileName + " failed")
      return false
    }
    else
    {
      redisHandler.hset(FILEDIR, fileName, fileName)
      setFileMetaData(fileName, attr)
      log.debug("Created file :" + fileName + " On meta store")
      return true
    }

  }

  def addChunk(fileName: String, chunkSeq: Double, chunkId: String) =
  {
    val returnVal = redisHandler.zadd(schemeFileNameForChunks(fileName), chunkSeq, chunkId)
    log.debug("Added chunk FileName :" + fileName + " ChunkSeq : " + chunkSeq.toString
      + " ChunkID : " + chunkId.toString + " ReturnVal of Operation : " + returnVal.toString)
  }

  def addChunk(fileName: String, scoreMembers: Map[java.lang.Double, String]) =
  {
    val returnVal = redisHandler.zadd(schemeFileNameForChunks(fileName), mapAsJavaMap(scoreMembers))
    log.debug("Added chunk FileName :" + fileName + " Map [ChunkSeq, ChunkId] : " + scoreMembers.toString()
      + " ReturnVal of Operation : " + returnVal.toString)
  }

  def setFileStatus(fileName: String, status: FileStatus.Value, opCount: Int) =
  {
    log.info("Received setFileStatus for " + fileName + " Status : " + status + " OpCnt " + opCount)
    val op = redisHandler.hget(schemeFileNameForDir(fileName), FileFields.STATUS.toString)

    status match
    {
      case FileStatus.IDLE if opCount == 0 =>
        redisHandler.hset(schemeFileNameForDir(fileName), FileFields.STATUS.toString, FileStatus.IDLE.toString)
        redisHandler.hset(schemeFileNameForDir(fileName), FileFields.OPCNT.toString, 0.toString)
        log.info("Setting File Status " + fileName + " to IDLE")

      case FileStatus.WRITING if opCount == -1 =>
        val opCnt = redisHandler.hget(schemeFileNameForDir(fileName), FileFields.OPCNT.toString).toInt
        if (opCnt <= 1 &&
          op == FileStatus.WRITING.toString)
          setFileStatus(fileName, FileStatus.IDLE, 0)
        else
          log.error("SetFileStatus -- FileName : " + fileName + " FileStatus : " + status.toString
            + " OpCnt : " + opCount.toString + "Current Status : " + op)

      case FileStatus.WRITING if opCount == 1 =>
        val opCnt = redisHandler.hget(schemeFileNameForDir(fileName), FileFields.OPCNT.toString).toInt
        if (op == FileStatus.IDLE.toString)
        {
          redisHandler.hset(schemeFileNameForDir(fileName), FileFields.STATUS.toString, FileStatus.WRITING.toString)
          redisHandler.hset(schemeFileNameForDir(fileName), FileFields.OPCNT.toString, (opCnt + 1).toString)
          log.info("Setting File Status " + fileName + " to WRITING")
        }
        else
        {
          log.warn("SetFileStatus -- FileName : " + fileName + " FileStatus : " + status.toString
            + " OpCnt : " + opCount.toString + "Current Status : " + op)
        }

      case FileStatus.READING if opCount == -1 =>
        val opCnt = redisHandler.hget(schemeFileNameForDir(fileName), FileFields.OPCNT.toString).toInt
        if (op == FileStatus.READING.toString)
        {
          if (opCnt <= 1)
            setFileStatus(fileName, FileStatus.IDLE, 0)
          else
          {
            redisHandler.hset(schemeFileNameForDir(fileName), FileFields.STATUS.toString, FileStatus.READING.toString)
            redisHandler.hset(schemeFileNameForDir(fileName), FileFields.OPCNT.toString, (opCnt - 1).toString)
            log.info("Setting File Status " + fileName + " to Reading with decremented opCnt")
          }
        }
        else
          log.error("SetFileStatus -- FileName : " + fileName + " FileStatus : " + status.toString
            + " OpCnt : " + opCount.toString + "Current Status : " + op)

      case FileStatus.READING if opCount == 1 =>
        val opCnt = redisHandler.hget(schemeFileNameForDir(fileName), FileFields.OPCNT.toString).toInt
        val op = redisHandler.hget(schemeFileNameForDir(fileName), FileFields.STATUS.toString)
        if (op == FileStatus.IDLE.toString ||
          op == FileStatus.READING.toString)
        {
          redisHandler.hset(schemeFileNameForDir(fileName), FileFields.STATUS.toString, FileStatus.READING.toString)
          redisHandler.hset(schemeFileNameForDir(fileName), FileFields.OPCNT.toString, (opCnt + 1).toString)
          log.info("Setting File Status " + fileName + " to Reading")
        }
        else
        {
          log.warn("SetFileStatus -- FileName : " + fileName + " FileStatus : " + status.toString
            + " OpCnt : " + opCount.toString + "Current Status : " + op)
        }

      case FileStatus.DELETING if opCount == -1 =>
        val opCnt = redisHandler.hget(schemeFileNameForDir(fileName), FileFields.OPCNT.toString).toInt
        if (opCnt <= 1 &&
          op == FileStatus.DELETING.toString)
          setFileStatus(fileName, FileStatus.IDLE, 0)
        else
          log.error("SetFileStatus -- FileName : " + fileName + " FileStatus : " + status.toString
            + " OpCnt : " + opCount.toString + "Current Status : " + op)

      case FileStatus.DELETING if opCount == 1 =>
        val opCnt = redisHandler.hget(schemeFileNameForDir(fileName), FileFields.OPCNT.toString).toInt
        val op = redisHandler.hget(schemeFileNameForDir(fileName), FileFields.STATUS.toString)
        if (op == FileStatus.IDLE.toString)
        {
          redisHandler.hset(schemeFileNameForDir(fileName), FileFields.STATUS.toString, FileStatus.DELETING.toString)
          redisHandler.hset(schemeFileNameForDir(fileName), FileFields.OPCNT.toString, (opCnt + 1).toString)
          log.info("Setting File Status " + fileName + " to Deleting")
        }
        else
        {
          log.warn("SetFileStatus -- FileName : " + fileName + " FileStatus : " + status.toString
            + " OpCnt : " + opCount.toString + "Current Status : " + op)
        }
    }
  }

  def setFileMetaData(fileName: String, attr: FileMetaData) =
  {
    redisHandler.hset(schemeFileNameForDir(fileName), FileFields.NAME.toString, fileName)
    redisHandler.hset(schemeFileNameForDir(fileName), FileFields.SIZE.toString, attr.size.toString)
    updateFileChunkCount(fileName, 0)
    setFileStatus(fileName, FileStatus.IDLE, 0)
    log.debug("Setting file metadata : " + attr.toString)
  }

  def updateFileChunkCount(fileName: String, chunkCnt: Int) =
  {
    val returnValue = redisHandler.hset(schemeFileNameForDir(fileName), FileFields.NUMCHUNKS.toString, chunkCnt.toString)
    log.debug("updating File Chunk count for file name : " + fileName
      + " with value :" + chunkCnt.toString + " with return value : " + returnValue.toString)

  }

  def getFileStatus(fileName: String) =
  {
    val returnValue = FileStatus.withName(redisHandler.hget(schemeFileNameForDir(fileName), FileFields.STATUS.toString))
    log.debug("File status for : " + fileName + " Status : " + returnValue.toString)
    returnValue
  }

  def getFileSize(fileName: String): Int =
  {
    val returnVal = redisHandler.hget(schemeFileNameForDir(fileName), FileFields.SIZE.toString).toInt
    log.debug("FileSize for fileName : " + fileName + "Size : " + returnVal.toString)
    returnVal
  }

  def listChunk(fileName: String) =
  {
    val returnVal = asScalaSet(redisHandler.zrangeByScore(schemeFileNameForChunks(fileName),
      Double.NegativeInfinity, Double.PositiveInfinity))

    log.debug("List of chunk for fileName : " + fileName + " Chunks : " + returnVal.toString())

    returnVal

  }

  def listFiles(): HashMap[String, String] =
  {
    val fileMap = redisHandler.hgetAll(FILEDIR)
    val returnVal = new HashMap[String, String]()

    fileMap foreach
      {
        case (k, v) => returnVal.put(k, redisHandler.hget(schemeFileNameForDir(k), FileFields.STATUS.toString))
      }

    log.debug("List of files : " + returnVal.toString())
    return returnVal
  }

  def deleteFile(fileName: String): Boolean =
  {
      redisHandler.del(schemeFileNameForChunks(fileName))
      log.debug("Deleting file from metaStore : " + fileName)
      return true
  }

}