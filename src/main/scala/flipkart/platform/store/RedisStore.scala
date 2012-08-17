package flipkart.platform.store

import akka.pattern.ask
import akka.util.Timeout
import akka.util.duration._
import akka.dispatch.Await
import com.redis.RedisClientPool
import flipkart.platform.file._
import flipkart.platform.actor.{FileStateMsg, SpeedActorSystem}
import akka.actor.{ActorRef, Props, Actor}
import collection.immutable.TreeSet
import collection.mutable.{LinkedList, HashMap}


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

  def setFileCurrentVersion(fileName: String, version: Int)
  {
    redisPool.withClient
    {
      redisHandler => if (getCurrentVersion(fileName) < version)
      {
        redisHandler.hset(MetaStoreUtil.schemeFileMap(fileName),
          FileMapFileNameFields.CurrentVersion.toString, version)
        log.info("Setting Current Version of File " + fileName + " to " + version)
      }
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

  def listChunkForCurrentVersion(fileName: String) =
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

  def listChunkForFileId (fileId : String) : List[String] =
  {
    redisPool.withClient
    {
      redisHandler =>
        val returnVal = redisHandler.zrangebyscore(MetaStoreUtil.schemeFileChunk(fileId),
          Double.NegativeInfinity, true, Double.PositiveInfinity, true, None) match {
          case Some(x) => x
          case None => List[String]()
        }
        log.debug("List of chunk for fileName : " + fileId + " Chunks : " + returnVal.toString())
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

  def rm (fileId :String) : List[String] =
  {
    var dataChunkList :List[String] = null
    redisPool.withClient{
      redisHandler => {
        redisHandler.hgetall(MetaStoreUtil.schemeFileNameVersion(fileId)) foreach {
          fileVersionMap => fileVersionMap foreach {
            case (fileName, version) => if (getCurrentVersion(fileName) > version.toInt)
            {
              setFileStatus(fileName, version.toInt, FileStatus.InActive)
              dataChunkList = listChunkForFileId(fileId)
              redisHandler.srem(MetaStoreUtil.schemeFileMapVersionSet(fileName), version)
              redisHandler.del(MetaStoreUtil.schemeFileNameVersion(fileId))
              redisHandler.del(MetaStoreUtil.schemeFileChunk(fileId))
            }
          }
        }
      }
    }
    return dataChunkList
  }

  def addReadActorToActiveSet (actorRef : String)
  {
    redisPool.withClient {
      redisHandle =>
        redisHandle.sadd(MetaStoreUtil.schemeActiveReadActorSet(), actorRef)
    }
    log.debug("Added Read Actor to Set " + actorRef)
  }

  def getReadActorActiveSet () : Option[Set[Option[String]]] =
  {
    redisPool.withClient {
      redisHandle =>
        redisHandle.smembers(MetaStoreUtil.schemeActiveReadActorSet())
    }
  }

  def removeReadActor (actorRef : String)
  {
    redisPool.withClient {
      redisHandle => redisHandle.pipeline {
        redisClient => redisClient.srem(MetaStoreUtil.schemeActiveReadActorSet(), actorRef)
                       redisClient.del(MetaStoreUtil.schemeActiveReadActorMap(actorRef))
      }
    }
    log.debug("Removed Read Actor " + actorRef)
  }

  def updateReadActorEpoch (actorRef : String, fileName : String,  version : Int)
  {
    redisPool.withClient {
      redisHandle =>
        redisHandle.hset(MetaStoreUtil.schemeActiveReadActorMap(actorRef),
            MetaStoreUtil.schemeFileID(fileName, version), System.currentTimeMillis())
    }

    log.debug("Updated ReadActorEpoch " + actorRef + " For FileID : " + fileName + version)
  }

  def getReadActorAccess (actorRef : String) : Option[Map[String, String]] =
  {
    redisPool.withClient {
      redisHandle =>
        redisHandle.hgetall(MetaStoreUtil.schemeActiveReadActorMap(actorRef))
    }
  }

  def updateReadFileCount (fileID:String,  value:Int)
  {
    redisPool.withClient {
      redisHandle => redisHandle.zincrby(MetaStoreUtil.schemeFileReadCount(), value, fileID)
    }
    log.debug("UpdateReadFileCount : FileID " + fileID + " by :" +value)
  }

  def getReadFileCountSet () : List[(String, Double)] =
  {
    redisPool.withClient {
      redisHandle =>
        redisHandle.zrangebyscoreWithScore(MetaStoreUtil.schemeFileReadCount(),
                Double.NegativeInfinity, true, Double.PositiveInfinity, true, None) match {
          case Some(x) => x
          case None => List[(String, Double)]()
        }
    }
  }

  def addWriteActorToActiveSet (actorRef : String)
  {
    redisPool.withClient {
      redisHandle =>
        redisHandle.sadd(MetaStoreUtil.schemeActiveWriteActorSet(), actorRef)
    }

    log.debug("Added Write Actor to Set " + actorRef)
  }

  def getWriteActorActiveSet () : Option[Set[Option[String]]] =
  {
    redisPool.withClient {
      redisHandle =>
        redisHandle.smembers(MetaStoreUtil.schemeActiveWriteActorSet())
    }
  }

  def removeWriteActor (actorRef : String)
  {
    redisPool.withClient {
      redisHandle => redisHandle.pipeline {
        redisClient => redisClient.srem(MetaStoreUtil.schemeActiveWriteActorSet(), actorRef)
                       redisClient.del(MetaStoreUtil.schemeActiveWriteActorMap(actorRef))
      }
    }
    log.debug("Removed Write Actor " + actorRef)
  }

  def updateWriteActorEpoch (actorRef : String, fileName : String,  version : Int)
  {
    redisPool.withClient {
      redisHandle =>
        redisHandle.hset(MetaStoreUtil.schemeActiveWriteActorMap(actorRef),
            MetaStoreUtil.schemeFileID(fileName, version), System.currentTimeMillis())
    }

    log.debug("Updated WriteActorEpoch " + actorRef + " For FileID : " + fileName + version)
  }

  def getWriteActorAccess (actorRef : String) : Option[Map[String, String]] =
  {
    redisPool.withClient {
      redisHandle =>
        redisHandle.hgetall(MetaStoreUtil.schemeActiveWriteActorMap(actorRef))
    }
  }

  def updateWriteFileCount (fileID:String,  value:Int)
  {
    redisPool.withClient {
      redisHandle => redisHandle.zincrby(MetaStoreUtil.schemeFileWriteCount(), value, fileID)
    }

    log.debug("UpdateWriteFileCount : FileID " + fileID + " by :" +value)
  }

  def getWriteFileCountSet () : List[(String, Double)] =
  {
    redisPool.withClient {
      redisHandle =>
        redisHandle.zrangebyscoreWithScore(MetaStoreUtil.schemeFileWriteCount(),
                Double.NegativeInfinity, true, Double.PositiveInfinity, true, None) match {
          case Some(x) => x
          case None => List[(String, Double)]()
        }
    }
  }

}