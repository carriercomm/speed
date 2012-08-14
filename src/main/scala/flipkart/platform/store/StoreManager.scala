package flipkart.platform.store

import flipkart.platform.{Speed, LightningConfig}
import flipkart.platform.file.{FileStatus, FileMetaData}
import java.nio.ByteBuffer
import flipkart.platform.randomGenerator.RandomGenerator
import java.io.{IOException, InputStream}
import akka.actor.{Props, Actor}
import akka.pattern.ask
import flipkart.platform.buffer.{SpeedBufStatus, SpeedBuf, UnBoundedFifoBuf}
import akka.util.Timeout
import akka.util.duration._
import akka.routing.RoundRobinRouter
import flipkart.platform.actor.{WriteMasterMsg, WriteMasterActor, SpeedActorSystem}


/**
 * Created by IntelliJ IDEA.
 * User: vivekys
 * Date: 03/08/12
 * Time: 3:48 PM
 * To change this template use File | Settings | File Templates.
 */

class StoreManager(config: LightningConfig) extends Speed
{

  implicit val timeout = Timeout(60 seconds)

  val metaStore = new RedisStore(config.metaStoreHost, config.metaStorePort)

  val dataStore = new MembaseStore(config.dataStoreHost, config.dataStoreBucket)

  val chunkSize = config.dataChunkSize

  def create(fileName: String) =
  {
    metaStore.createFile(fileName)
  }

  def write(fileName : String, metaData : FileMetaData, inputStream : InputStream) =
  {
    if (isExist(fileName) == false)
    {
      create(fileName)
    }
    val version = metaStore.setFileMetaData(fileName, metaData)

    val system = SpeedActorSystem.getActorSystem()

    val writeMasterActor = system.actorOf(Props[WriteMasterActor])

    writeMasterActor ! WriteMasterMsg(fileName, version, metaData, metaStore, dataStore, config, inputStream)
  }

  def read(fileName: String): SpeedBuf =
  {
    if (metaStore.isExist(fileName) && metaStore.getFileStatus(fileName) == FileStatus.Active)
    {
      //Register the reader for book keeper

      var buffer = new UnBoundedFifoBuf

      case object Start

      class Worker extends Actor
      {
        log.info("Created Worker for reading data chunk of file " + fileName)
        buffer.bufWriteComplete = SpeedBufStatus.NO

        def act()
        {
          val start: Long = System.currentTimeMillis()
          val prefetchSize = config.preFetchSize
          val chunkList = metaStore.listChunk(fileName).toSeq
          val chuckIter = chunkList grouped prefetchSize
          chuckIter.foreach( grp => {
            val kv = dataStore.multiGetData(grp.toArray)
            grp.foreach( item => {
              buffer.write(kv(item))
              buffer.bufReadable = SpeedBufStatus.YES
            })
          })
          buffer.bufWriteComplete = SpeedBufStatus.YES
          log.info("Completed reading the file " + fileName + " in " + (System.currentTimeMillis - start) + "ms")
        }

        protected def receive =
        {
          case Start => act()
                        context.stop(self)
        }
      }
      val system = SpeedActorSystem.getActorSystem()
      val worker = system.actorOf(Props(new Worker()))
      worker ! Start

      return buffer
    }
    else
    {
      throw new IOException("Cannot read the file")
    }
  }

  def delete(fileName: String): Boolean =
  {
    //Yet to implement
    true
  }

  def ls(): Array[Pair[String, FileStatus.Value]] =
  {
    val status = metaStore.listFiles()
    val returnArr = new Array[Pair[String, FileStatus.Value]](status.size)
    var i = 0
    for ((k, v) <- status)
    {
      returnArr(i) = Pair[String, FileStatus.Value](k, FileStatus.withName(v))
    }
    return returnArr
  }

  def isExist (fileName : String) = {
    metaStore.isExist(fileName)
  }
}