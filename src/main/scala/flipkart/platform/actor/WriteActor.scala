package flipkart.platform.actor

import com.codahale.logula.Logging
import java.nio.ByteBuffer
import flipkart.platform.store.{RedisStore, MembaseStore}
import akka.actor.{Props, Actor}
import akka.routing.RoundRobinRouter
import flipkart.platform.randomGenerator.RandomGenerator
import flipkart.platform.file.{FileStatus, FileMetaData}
import flipkart.platform.LightningConfig
import java.io.InputStream

/**
 * Created by IntelliJ IDEA.
 * User: vivekys
 * Date: 14/08/12
 * Time: 2:59 PM
 * To change this template use File | Settings | File Templates.
 */

case class WriteMasterMsg(fileName: String,
                          version: Int,
                          metaData: FileMetaData,
                          inputStream: InputStream)

class WriteMasterActor(val metaStore : RedisStore, val dataStore : MembaseStore,
                                                   val config : LightningConfig) extends Actor with Logging
{
  log.info("Created WriteMaster")
  val workerRouter = context.actorOf(Props[WriteWorkerActor].withRouter(RoundRobinRouter(100)))

  protected def receive =
  {
    case msg: WriteMasterMsg => sender ! write(msg.fileName, msg.version, msg.metaData, msg.inputStream)

  }

  def write (fileName: String, version: Int, metaData: FileMetaData, inputStream: InputStream) {
    val fileSize = metaData.size
    val chunkSize = config.dataChunkSize
    val chunkCnt = (fileSize / chunkSize +
      (if (fileSize % chunkSize == 0)
        0
      else
        1)).toInt
    //Loop for 1 to chunkCnt - 1 which has full chunkSize &
    //for the last chunk the size will be fileSize - chunkSize * (chunkCnt - 1)
    for (i <- 1 to chunkCnt - 1)
    {
      var byteBuffer = ByteBuffer.allocate(chunkSize)
      for (j <- 1 to chunkSize)
      {
        var byte = inputStream.read()
        if (byte == -1)
          log.error("Something got screwed")
        else
          byteBuffer.put(byte.toByte)
      }
      var key = RandomGenerator.generate() + fileName + version
      metaStore.addChunk(fileName, version, i.asInstanceOf[Double], key)
      workerRouter ! WriteWorkerMsg(key, byteBuffer, dataStore)
      metaStore.updateWriteActorEpoch(self.toString(), fileName, version)
    }
    //Read until EOF
    val finalChunkSize = fileSize - (chunkSize * (chunkCnt - 1))
    val finalByteBuffer = ByteBuffer.allocate(finalChunkSize.toInt)
    var byte = inputStream.read()
    while (byte != -1)
    {
      finalByteBuffer.put(byte.toByte)
      byte = inputStream.read()
    }
    var key = RandomGenerator.generate() + fileName + version
    metaStore.addChunk(fileName, version, chunkCnt.asInstanceOf[Double], key)
    workerRouter ! WriteWorkerMsg(key, finalByteBuffer, dataStore)
    metaStore.updateFileChunkCount(fileName, version, chunkCnt)
    metaStore.setFileStatus(fileName, version, FileStatus.Active)
    metaStore.setFileCurrentVersion(fileName, version)
    metaStore.updateWriteActorEpoch(self.toString(), fileName, version)
  }
}

case class WriteWorkerMsg(key: String, buffer: ByteBuffer, dataStore: MembaseStore)

class WriteWorkerActor extends Actor with Logging
{
  log.info("Created WriteWorkerActor")

  protected def receive =
  {
    case WriteWorkerMsg(key, buffer, dataStore) => sender ! dataStore.addData(key, buffer.array())
                                                   buffer.clear()
  }
}