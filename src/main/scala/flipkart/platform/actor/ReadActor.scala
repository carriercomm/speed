package flipkart.platform.actor

import akka.actor.Actor
import com.codahale.logula.Logging
import flipkart.platform.store.{MembaseStore, RedisStore}
import flipkart.platform.LightningConfig
import flipkart.platform.buffer.{SpeedBufStatus, UnBoundedFifoBuf}

/**
 * Created by IntelliJ IDEA.
 * User: vivekys
 * Date: 14/08/12
 * Time: 2:59 PM
 * To change this template use File | Settings | File Templates.
 */

case class ReadMasterMsg(fileName: String, buffer: UnBoundedFifoBuf)

class ReadMasterActor(val metaStore: RedisStore, val dataStore: MembaseStore,
                      val config: LightningConfig) extends Actor with Logging
{
  protected def receive =
  {
    case msg: ReadMasterMsg => read(msg.fileName, msg.buffer)
  }

  private def read(fileName: String, buffer: UnBoundedFifoBuf)
  {
    val start: Long = System.currentTimeMillis()
    val prefetchSize = config.preFetchSize
    val version = metaStore.getCurrentVersion(fileName)
    val chunkList = metaStore.listChunkForCurrentVersion(fileName).toSeq
    val chuckIter = chunkList grouped prefetchSize
    chuckIter.foreach(grp =>
    {
      val kv = dataStore.multiGetData(grp.toArray)
      grp.foreach(item =>
      {
        buffer.write(kv(item))
        buffer.bufReadable = SpeedBufStatus.YES
      })
      metaStore.updateReadActorEpoch(self.toString(), fileName, version)
    })
    buffer.bufWriteComplete = SpeedBufStatus.YES
    log.info("Completed reading the file " + fileName + " in " + (System.currentTimeMillis - start) + "ms")
  }
}