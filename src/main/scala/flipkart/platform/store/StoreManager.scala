package flipkart.platform.store

import flipkart.platform.{Speed, LightningConfig}
import flipkart.platform.file.{FileStatus, FileMetaData}
import java.io.{IOException, InputStream}
import akka.actor.Props
import flipkart.platform.buffer.{SpeedBuf, UnBoundedFifoBuf}
import akka.util.duration._
import flipkart.platform.actor._
import java.util.concurrent.TimeUnit
import akka.util.{Duration, Timeout}


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
  val actorSystem = SpeedActorSystem.getActorSystem()
  val janitorActorRef = actorSystem.actorOf(Props(new JanitorActor(metaStore, dataStore)))
  actorSystem.scheduler.schedule(Duration(2, TimeUnit.MINUTES), Duration(2, TimeUnit.MINUTES),
        janitorActorRef, Read)

  actorSystem.scheduler.schedule(Duration(2, TimeUnit.MINUTES), Duration(2, TimeUnit.MINUTES),
        janitorActorRef, Write)

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

    val writeMasterActor = system.actorOf(Props(new WriteMasterActor(metaStore, dataStore, config)))

    writeMasterActor ! WriteMasterMsg(fileName, version, metaData, inputStream)
    metaStore.addWriteActorToActiveSet(writeMasterActor.toString())
  }

  def read(fileName: String): SpeedBuf =
  {
    if (metaStore.isExist(fileName) && metaStore.getFileStatus(fileName) == FileStatus.Active)
    {

      var buffer = new UnBoundedFifoBuf

      val system = SpeedActorSystem.getActorSystem()
      val worker = system.actorOf(Props(new ReadMasterActor(metaStore, dataStore, config)))
      worker ! ReadMasterMsg(fileName, buffer)
      metaStore.addReadActorToActiveSet(worker.toString())
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