package flipkart.platform.store

import scala.collection.JavaConversions._
import redis.clients.jedis._
import flipkart.platform.file.{FileStatus, FileFields, FileMetaData}

/**
 * Created by IntelliJ IDEA.
 * User: vivekys
 * Date: 02/08/12
 * Time: 1:42 PM
 * To change this template use File | Settings | File Templates.
 */

class RedisStore (val host:String, val port : Int) extends MetaStore{
  val FILEMAP = "FILEMAP"

  val FILEDIR = "FILEDIR"

  val FILECHUNK = "FILECHUNK"

  val redisHandler = new Jedis(host, port)

  private def schemeFileNameForDir(fileName : String) = FILEMAP + fileName

  private def schemeFileNameForChunks(fileName : String) = FILECHUNK + fileName

  def setFileMetaData (fileName : String, attr : FileMetaData) = {
    redisHandler.hset(schemeFileNameForDir(fileName), FileFields.NAME.toString, fileName)
    redisHandler.hset(schemeFileNameForDir(fileName), FileFields.SIZE.toString, attr.size.toString)
    updateFileChunkCount(fileName, 0)
    setFileStatus(fileName, FileStatus.WRITING)
  }

  def updateFileChunkCount(fileName:String, chunkCnt:Int) = {
    redisHandler.hset(schemeFileNameForDir(fileName), FileFields.NUMCHUNKS.toString, chunkCnt.toString)
  }

  def createFile(fileName: String, attr: FileMetaData) : Boolean =
  {
    if (redisHandler.hexists(FILEDIR, fileName))
      return false
    else
    {
      redisHandler.hset(FILEDIR, fileName, fileName)
      setFileMetaData(fileName, attr)
      return true
    }

  }

  def addChunk(fileName: String, chunkSeq : Double, chunkId: String) =
  {
    redisHandler.zadd(schemeFileNameForChunks(fileName), chunkSeq, chunkId)
  }

  def addChunk(fileName: String, scoreMembers : Map[java.lang.Double, String]) = {
    redisHandler.zadd(schemeFileNameForChunks(fileName), mapAsJavaMap(scoreMembers))
  }

  def setFileStatus(fileName: String, status: FileStatus.Value) = {
    redisHandler.hset(schemeFileNameForDir(fileName), FileFields.STATUS.toString, status.toString)
  }

  def getFileStatus(fileName:String) = {
    FileStatus.withName(redisHandler.hget(schemeFileNameForDir(fileName), FileFields.STATUS.toString))
  }

  def getFileSize(fileName:String) : Int = {
    redisHandler.hget(schemeFileNameForDir(fileName), FileFields.SIZE.toString).toInt
  }

  def listChunk(fileName: String) = {
    asScalaSet(redisHandler.zrangeByScore(schemeFileNameForChunks(fileName),
        Double.NegativeInfinity, Double.PositiveInfinity))
  }

  def listFiles() : Map[String, String] = {
    val fileMap = redisHandler.hgetAll(FILEDIR)
    val returnVal = Map[String, String]()

    fileMap foreach {case (k,v) => returnVal.put(k, redisHandler.hget(schemeFileNameForDir(k), FileFields.STATUS.toString))}

    return returnVal
  }

  def deleteFile(fileName: String) :Boolean = {
    if (redisHandler.hget(schemeFileNameForDir(fileName), FileFields.STATUS.toString) == FileStatus.IDLE)
    {
      setFileStatus(fileName, FileStatus.DELETING)
      redisHandler.del(schemeFileNameForChunks(fileName))
      return true
    }
    else
    {
      return false
    }
  }

}