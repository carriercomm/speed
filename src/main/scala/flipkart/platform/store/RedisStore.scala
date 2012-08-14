package flipkart.platform.store

import collection.mutable.HashMap
import akka.actor.{Props, Actor}
import akka.pattern.ask
import akka.util.Timeout
import akka.util.duration._
import akka.dispatch.Await
import com.redis.RedisClientPool
import flipkart.platform.file._
import flipkart.platform.actor.{FileStateMsg, SpeedActorSystem}


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

  def createFile(fileName: String): Boolean =
  {
    redisPool.withClient
    {
      redisHandler =>
        if (isExist(fileName))
        {
          log.debug("Creation of file :" + fileName + " failed")
          return false
        }
        else
        {
          redisHandler.sadd(MetaStoreUtil.schemeFileDir(), fileName)
          log.debug("Created file :" + fileName + " On meta store")
          return true
        }
    }
  }

  def addChunk(fileName: String, version : Int, chunkSeq: Double, chunkId: String)
  {
    redisPool.withClient
    {
      redisHandler =>
        val returnVal = redisHandler.zadd(MetaStoreUtil.schemeFileChunk(fileName, version), chunkSeq, chunkId)
        log.debug("Added chunk FileName :" + fileName + version + " ChunkSeq : " + chunkSeq.toString
          + " ChunkID : " + chunkId.toString + " ReturnVal of Operation : " + returnVal.toString)
    }
  }

  def setFileStatus(fileName: String, version: Int, status: FileStatus.Value) =
  {
    redisPool.withClient
    {
      redisHandler =>
        log.info("Received setFileStatus for " + fileName + " Version :" + version +" Status : " + status)
        implicit val timeout = Timeout(10 seconds)
        val actor = SpeedActorSystem.getFileStateActor()

        val future = actor ? FileStateMsg(fileName, version, status, redisHandler)
        val result = Await.result(future, timeout.duration).asInstanceOf[Boolean]
        log.info("SetFileStatus - FileName : " + fileName + " Version :" + version +" Status : " + status +
          " result :" + result)
        result
    }
  }

  def setFileMetaData(fileName: String, attr: FileMetaData) : Int =
  {
    redisPool.withClient
    {
      redisHandler =>
        val result = redisHandler.pipeline {
          pipelineClient =>
            pipelineClient.hset(MetaStoreUtil.schemeFileMap(fileName), FileMapFileNameFields.Name.toString, fileName)
            pipelineClient.hset(MetaStoreUtil.schemeFileMap(fileName), FileMapFileNameFields.Size.toString, attr.size)
            pipelineClient.hincrby(MetaStoreUtil.schemeFileMap(fileName),
                                FileMapFileNameFields.Version.toString, 1)

        } get

        val version = result(2).asInstanceOf[Option[Int]].getOrElse(-1)
        redisHandler.pipeline
        {
          pipelineClient =>
            pipelineClient.zadd(MetaStoreUtil.schemeFileMapVersionSet(fileName), version, version.toString)
            pipelineClient.hset(MetaStoreUtil.schemeFileNameVersion(fileName, version),
                              FileMapFileNameVersionFields.Version.toString, version)

        }
        setFileStatus(fileName, version, FileStatus.InActive)
        updateFileChunkCount(fileName, version, 0)

        log.debug("Setting file metadata : " + attr.toString)
        version
    }
  }

  def setFileCurrentVersion(fileName:String, version:Int)
  {
    redisPool.withClient
    {
      redisHandler =>
        redisHandler.hset(MetaStoreUtil.schemeFileMap(fileName),
                                  FileMapFileNameFields.CurrentVersion.toString, version)
        log.info("Setting Current Version of File " + fileName + " to " + version)
    }
  }

  def updateFileChunkCount(fileName: String, version: Int,  chunkCnt: Int)
  {
    redisPool.withClient
    {
      redisHandler =>
        val returnValue = redisHandler.hset(MetaStoreUtil.schemeFileNameVersion(fileName, version),
          FileMapFileNameVersionFields.NumChunks.toString, chunkCnt.toString)
        log.debug("Updating File Chunk count for file name : " + fileName + " Version : " + version
          + " with value :" + chunkCnt.toString + " with return value : " + returnValue.toString)
    }
  }

  def getFileStatus(fileName : String,  version : Int) = {
    redisPool.withClient
    {
      redisHandler =>
        val returnValue = FileStatus.withName(redisHandler.hget(MetaStoreUtil.schemeFileNameVersion(
                             fileName, version), FileMapFileNameVersionFields.State.toString)
                             .getOrElse(FileStatus.UnKnown.toString))
        log.debug("File status for : " + fileName + " Status : " + returnValue)
        returnValue

    }
  }
  def getFileStatus(fileName: String) =
  {
        val currentVersion = getCurrentVersion(fileName)
        val returnValue = getFileStatus(fileName, currentVersion)
        log.debug("File status for : " + fileName + " Status : " + returnValue.toString)
        returnValue
  }

  def getFileSize(fileName: String): Int =
  {
    redisPool.withClient
    {
      redisHandler =>
        val returnVal = redisHandler.hget(MetaStoreUtil.schemeFileMap(fileName),
                                                  FileMapFileNameFields.Size.toString).getOrElse(1.toString)
        log.debug("FileSize for fileName : " + fileName + "Size : " + returnVal)
        returnVal.toInt
    }
  }

  def getCurrentVersion(fileName: String) =
  {
    redisPool.withClient
    {
      redisHandler =>
        redisHandler.hget(MetaStoreUtil.schemeFileMap(fileName),
          FileMapFileNameFields.CurrentVersion.toString).getOrElse(1.toString).toInt
    }
  }

  def listChunk(fileName: String) =
  {
    redisPool.withClient
    {
      redisHandler =>
        val currentVersion = getCurrentVersion(fileName)
        val returnVal = redisHandler.zrangebyscore(MetaStoreUtil.schemeFileChunk(fileName, currentVersion),
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
        val fileSet = redisHandler.smembers(MetaStoreUtil.schemeFileDir())
        val returnVal = new HashMap[String, String]()
        fileSet match {
          case Some (files) => files foreach {
                                      file => file match {
                                        case Some(x) => returnVal.put(x, getFileStatus(x).toString)
                                        case None =>
                                      }
                                    }
          case None => Map[String,  String] ()
        }
        log.debug("List of files : " + returnVal.toString())
        return returnVal
    }
  }

  def deleteFile(fileName: String)
  {
    //Yet to implement
  }

  def isExist (fileName : String) = {
    redisPool.withClient {
      redisHandler =>
        redisHandler.sismember(MetaStoreUtil.schemeFileDir(), fileName)
    }
  }


}