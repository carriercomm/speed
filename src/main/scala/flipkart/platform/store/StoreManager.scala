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

  def create(fileName: String, metaData: FileMetaData) =
  {
    metaStore.createFile(fileName, metaData)
  }

  def write(fileName: String, inputStream: InputStream) =
  {
    if (metaStore.getFileStatus(fileName) == FileStatus.IDLE)
    {
      metaStore.setFileStatus(fileName, FileStatus.WRITING, 1)

      val fileSize = metaStore.getFileSize(fileName)
      val chunkCnt = fileSize / chunkSize +
        (if (fileSize % chunkSize == 0)
          0
        else
          1)
      //Loop for 1 to chunkCnt - 1 which has full chunkSize &
      //for the last chunk the size will be fileSize - chunkSize * (chunkCnt - 1)
      val byteBuffer = ByteBuffer.allocate(chunkSize)
      for (i <- 1 to chunkCnt - 1)
      {
        for (j <- 1 to chunkSize)
        {
          var byte = inputStream.read()
          if (byte == -1)
            log.error("Something got screwed")
          else
            byteBuffer.put(byte.toByte)
        }
        var key = RandomGenerator.generate() + fileName
        metaStore.addChunk(fileName, i.asInstanceOf[Double], key)
        dataStore.addData(key, byteBuffer.array())
        byteBuffer.clear()
      }
      //Read until EOF
      val finalChunkSize = fileSize - (chunkSize * (chunkCnt - 1))
      val finalByteBuffer = ByteBuffer.allocate(finalChunkSize)
      var byte = inputStream.read()
      while (byte != -1)
      {
        finalByteBuffer.put(byte.toByte)
        byte = inputStream.read()
      }
      var key = RandomGenerator.generate() + fileName
      metaStore.addChunk(fileName, chunkCnt.asInstanceOf[Double], key)
      dataStore.addData(key, finalByteBuffer.array())
      metaStore.updateFileChunkCount(fileName, chunkCnt)
      metaStore.setFileStatus(fileName, FileStatus.WRITING, -1)
      finalByteBuffer.clear()
    }
    else
    {
      throw new IOException("Cannot write to file")
    }
  }

  def read(fileName: String): SpeedBuf =
  {
    val status = metaStore.getFileStatus(fileName)
    if (status == FileStatus.IDLE || status == FileStatus.READING)
    {
      metaStore.setFileStatus(fileName, FileStatus.READING, 1)

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
          metaStore.setFileStatus(fileName, FileStatus.READING, -1)
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
    if (metaStore.getFileStatus(fileName) == FileStatus.IDLE)
    {
      metaStore.setFileStatus(fileName, FileStatus.DELETING, 1)
      var chunkList = metaStore.listChunk(fileName)
      for (item <- chunkList)
        dataStore.deleteData(item)
      metaStore.deleteFile(fileName)
      return true
    }
    else
      return false
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
}