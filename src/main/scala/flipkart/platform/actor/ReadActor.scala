package flipkart.platform.actor

import com.codahale.logula.Logging
import flipkart.platform.store.{MembaseStore, RedisStore}
import flipkart.platform.LightningConfig
import flipkart.platform.buffer.{SpeedBufStatus, UnBoundedFifoBuf}
import akka.routing.RoundRobinRouter
import akka.dispatch._
import Future.flow
import akka.dispatch.Await
import akka.pattern.ask
import akka.util.Timeout
import akka.util.duration._
import akka.dispatch.Promise
import java.util.concurrent.Executors
import akka.actor.SupervisorStrategy.Restart
import akka.actor.{OneForOneStrategy, Props, Actor}


/**
 * Created by IntelliJ IDEA.
 * User: vivekys
 * Date: 14/08/12
 * Time: 2:59 PM
 * To change this template use File | Settings | File Templates.
 */

case class ReadMasterMsg(fileName: String, buffer: UnBoundedFifoBuf)

case class ReadWorkerMsg(keySet: Array[String])

class ReadWorker(val metaStore: RedisStore, val dataStore: MembaseStore) extends Actor
{
  protected def receive =
  {
    case msg: ReadWorkerMsg => sender ! dataStore.multiGetData(msg.keySet)
  }
}


class ReadMasterActor(val metaStore: RedisStore, val dataStore: MembaseStore,
                      val config: LightningConfig) extends Actor with Logging
{

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 10 seconds) {
    case _ => Restart
  }

  val readerConcurrencyFactor = config.readerConcurrencyFactor
  val system = SpeedActorSystem.getActorSystem()
  val workerRouter = system.actorOf(Props(new ReadWorker(metaStore, dataStore))
              .withRouter(RoundRobinRouter(readerConcurrencyFactor)))
  implicit val timeout = Timeout(10 seconds)
  implicit val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1000))

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
    val chunkGrpList = chunkList grouped prefetchSize toSeq

    scatterGather(chunkGrpList, buffer)

    buffer.bufWriteComplete = SpeedBufStatus.YES
    log.info("Completed reading the file " + fileName + " in " + (System.currentTimeMillis - start) + "ms")
  }

  def scatterGather(chuckGrpList: Seq[Seq[String]], buffer: UnBoundedFifoBuf)
  {
    val readerConcurrencyFactor = config.readerConcurrencyFactor
    val concGrpList = chuckGrpList grouped readerConcurrencyFactor toSeq

    concGrpList foreach (grp =>
    {
      val promises = List.fill(readerConcurrencyFactor)(
                                  Promise[scala.collection.mutable.Map[String, Array[Byte]]]())
      val resultMap = grp.zip(promises)

      resultMap.foreach
      {
        case (keySet, promise) =>
        {
          val result = ask(workerRouter, ReadWorkerMsg(keySet.toArray[String]))
            .mapTo[scala.collection.mutable.Map[String, Array[Byte]]]
          flow
          {
            promise << result
          }
        }
      }

      resultMap.foreach
      {
        case (key, res) =>
        {
          val result = Await.result(res, timeout.duration)
            .asInstanceOf[scala.collection.mutable.Map[String, Array[Byte]]]
          key foreach (k => {
            buffer.write(result(k))
          })
          buffer.bufReadable = SpeedBufStatus.YES
        }
      }
    })
  }
}