package flipkart.platform.store

import flipkart.platform.{Speed, LightningConfig}
import java.io.InputStream
import flipkart.platform.file.{FileStatus, FileMetaData}
import java.nio.ByteBuffer
import flipkart.platform.randomGenerator.RandomGenerator
import actors.Actor
import flipkart.platform.buffer.{SpeedBuf, UnBoundedFifoBuf}


/**
 * Created by IntelliJ IDEA.
 * User: vivekys
 * Date: 03/08/12
 * Time: 3:48 PM
 * To change this template use File | Settings | File Templates.
 */

class StoreManager (config : LightningConfig) extends Speed{
  val metaStore = new RedisStore(config.metaStoreHost, config.metaStorePort)

  val dataStore = new MembaseStore(config.dataStoreHost, config.dataStoreBucket)

  val chunkSize = config.dataChunkSize

  def create(fileName: String, metaData: FileMetaData) = {
    metaStore.createFile(fileName, metaData)
  }

  def write(fileName: String, inputStream: InputStream) = {
    if (metaStore.getFileStatus(fileName) == FileStatus.WRITING)
    {
      val fileSize = metaStore.getFileSize(fileName)
      val chunkCnt = fileSize/chunkSize +
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
            println("Something got screwed")
          else
            byteBuffer.put(byte.toByte)
        }
        var key = RandomGenerator.generate() + fileName
        metaStore.addChunk(fileName, i.asInstanceOf[Double], i.toString)
        dataStore.addData(key, byteBuffer.array())
        byteBuffer.clear()
      }
      //Read until EOF
      val finalChunkSize = fileSize - chunkSize * (chunkCnt - 1)
      val finalByteBuffer = ByteBuffer.allocate(finalChunkSize)
      var byte = inputStream.read()
      while (byte != -1)
      {
        finalByteBuffer.put(byte.toByte)
      }
      var key = RandomGenerator.generate() + fileName
      metaStore.addChunk(fileName, chunkCnt.asInstanceOf[Double], chunkCnt.toString)
      dataStore.addData(key, finalByteBuffer.array())
      metaStore.setFileStatus(fileName, FileStatus.READING)
    }
  }

  def read(fileName: String) : SpeedBuf = {
    val buffer = new UnBoundedFifoBuf

    class Worker (val buffer : UnBoundedFifoBuf) extends Actor {
      def act()
      {
        val prefetchSize = config.preFetchSize
        var chunkList = metaStore.listChunk(fileName)
        var numChunks = chunkList.size
        while (numChunks >= 0)
        {
          val rightSet = chunkList drop prefetchSize
          val leftSet = chunkList diff rightSet
          val kv = dataStore.multiGetData(leftSet.toArray)
          for (item <- leftSet)
          {
            buffer.write(kv(item))
            chunkList = chunkList - item
          }
          numChunks = numChunks - prefetchSize
        }
      }
    }
    val worker = new Worker(buffer)
    worker.start()
    return buffer
  }

  def delete(fileName: String) : Boolean = {
    if (metaStore.getFileStatus(fileName) == FileStatus.IDLE)
    {
      var chunkList = metaStore.listChunk(fileName)
      for (item <- chunkList)
        dataStore.deleteData(item)
      metaStore.deleteFile(fileName)
      return true
    }
    else
      false
  }

  def ls() : Array[Pair[String,  FileStatus.Value]] = {
    val status = metaStore.listFiles()
    val returnArr = new Array[Pair[String, FileStatus.Value]](status.size)
    var i = 0
    for ((k,v) <- status)
    {
      returnArr(i) = Pair[String,  FileStatus.Value](k,FileStatus.withName(v))
    }
    return returnArr
  }
}