package flipkart.platform.store

import flipkart.platform.file.{FileStatus, FileFields, FileMetaData}
import collection.mutable.HashMap
import akka.actor.{Props, Actor}
import akka.pattern.ask
import akka.util.Timeout
import akka.util.duration._
import akka.dispatch.Await
import com.redis.RedisClientPool


/**
 * Created by IntelliJ IDEA.
 * User: vivekys
 * Date: 02/08/12
 * Time: 1:42 PM
 * To change this template use File | Settings | File Templates.
 */

class RedisStore(val host: String, val port: Int) extends MetaStore
{
  val redisPool = new RedisClientPool(host, port)

  log.info("Creating RedisStore with = " + host + ":" + port)

  def createFile(fileName: String, attr: FileMetaData): Boolean =
  {
    redisPool.withClient
    {
      redisHandler =>
        if (redisHandler.hexists(MetaStoreUtil.FILEDIR, fileName))
        {
          log.debug("Creation of file :" + fileName + " failed")
          return false
        }
        else
        {
          redisHandler.hset(MetaStoreUtil.FILEDIR, fileName, fileName)
          setFileMetaData(fileName, attr)
          log.debug("Created file :" + fileName + " On meta store")
          return true
        }
    }
  }

  def addChunk(fileName: String, chunkSeq: Double, chunkId: String)
  {
    redisPool.withClient
    {
      redisHandler =>
        val returnVal = redisHandler.zadd(MetaStoreUtil.schemeFileNameForChunks(fileName), chunkSeq, chunkId)
        log.debug("Added chunk FileName :" + fileName + " ChunkSeq : " + chunkSeq.toString
          + " ChunkID : " + chunkId.toString + " ReturnVal of Operation : " + returnVal.toString)
    }
  }

  def setFileStatus(fileName: String, status: FileStatus.Value, opCount: Int) =
  {
    redisPool.withClient
    {
      redisHandler =>
        log.info("Received setFileStatus for " + fileName + " Status : " + status + " OpCnt " + opCount)
        implicit val timeout = Timeout(10 seconds)
        val actor = SpeedActorSystem.getFileStateActor()

        val future = actor ? FileStateMsg(fileName, status, opCount, redisHandler)
        val result = Await.result(future, timeout.duration).asInstanceOf[Boolean]
        log.info("SetFileStatus - FileName : " + fileName + " Status : " + status + " opCount :" + opCount +
          " result :" + result)
        result
    }
  }

  def setFileMetaData(fileName: String, attr: FileMetaData) =
  {
    redisPool.withClient
    {
      redisHandler =>
        redisHandler.hset(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.NAME.toString, fileName)
        redisHandler.hset(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.SIZE.toString, attr.size.toString)
        updateFileChunkCount(fileName, 0)
        setFileStatus(fileName, FileStatus.IDLE, 0)
        log.debug("Setting file metadata : " + attr.toString)
    }
  }

  def updateFileChunkCount(fileName: String, chunkCnt: Int)
  {
    redisPool.withClient
    {
      redisHandler =>
        val returnValue = redisHandler.hset(MetaStoreUtil.schemeFileNameForDir(fileName),
          FileFields.NUMCHUNKS.toString, chunkCnt.toString)
        log.debug("updating File Chunk count for file name : " + fileName
          + " with value :" + chunkCnt.toString + " with return value : " + returnValue.toString)
    }
  }

  def getFileStatus(fileName: String) =
  {
    redisPool.withClient
    {
      redisHandler =>
        val returnValue = FileStatus.withName(redisHandler.hget(MetaStoreUtil.schemeFileNameForDir(fileName),
          FileFields.STATUS.toString) match {
          case Some(x) => x
          case None => log.error("Unable to get file status")
                       FileStatus.UNKNOWN.toString
        })
        log.debug("File status for : " + fileName + " Status : " + returnValue.toString)
        returnValue
    }
  }

  def getFileSize(fileName: String): Int =
  {
    redisPool.withClient
    {
      redisHandler =>
        val returnVal = redisHandler.hget(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.SIZE.toString) match {
          case Some(x) => x.toInt
          case None => 0
        }
        log.debug("FileSize for fileName : " + fileName + "Size : " + returnVal.toString)
        returnVal
    }
  }

  def listChunk(fileName: String) =
  {
    redisPool.withClient
    {
      redisHandler =>
        val returnVal = redisHandler.zrangebyscore(MetaStoreUtil.schemeFileNameForChunks(fileName),
          Double.NegativeInfinity, true, Double.PositiveInfinity, true, None) match {
          case Some(x) => x
          case None => List[String]()
        }
        log.debug("List of chunk for fileName : " + fileName + " Chunks : " + returnVal.toString())

        returnVal
    }
  }

  def listFiles(): HashMap[String, String] =
  {
    redisPool.withClient
    {
      redisHandler =>
        val fileMap = redisHandler.hgetall(MetaStoreUtil.FILEDIR)
        val returnVal = new HashMap[String, String]()
        fileMap match {
          case Some(x) => x foreach
                            {
                                  case (k, v) => returnVal.put(k, redisHandler.hget(MetaStoreUtil
                                    .schemeFileNameForDir(k), FileFields.STATUS.toString) match {
                                        case Some(x) => x
                                        case None => null
                                  })
                            }
          case None => Map[String, String]()
        }

        log.debug("List of files : " + returnVal.toString())
        return returnVal
    }
  }

  def deleteFile(fileName: String)
  {
    redisPool.withClient
    {
      redisHandler =>
        redisHandler.del(MetaStoreUtil.schemeFileNameForChunks(fileName))
        log.debug("Deleting file from metaStore : " + fileName)
    }
  }

}