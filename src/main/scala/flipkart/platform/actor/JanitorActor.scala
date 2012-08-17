package flipkart.platform.actor

import flipkart.platform.store.{MembaseStore, RedisStore}
import akka.actor.Actor
import com.codahale.logula.Logging


/**
 * Created by IntelliJ IDEA.
 * User: vivekys
 * Date: 16/08/12
 * Time: 3:58 PM
 * To change this template use File | Settings | File Templates.
 */

case object Read
case object Write

class JanitorActor (metaStore : RedisStore, dataStore : MembaseStore) extends Actor with Logging {
  log.info("Created JanitorActor")

  val maxAccessTime = 120000
  protected def receive = {
    case Read => processReadMsg()
                 cleanInactiveFiles()
    case Write => processWriteMsg
  }

  private def processReadMsg ()
  {
    val readActorSetOpt = metaStore.getReadActorActiveSet()

    readActorSetOpt foreach {
      readActorSet => readActorSet foreach {
        readActorOpt => readActorOpt foreach {
          readActor => metaStore.getReadActorAccess(readActor) foreach  {
            accessMap => accessMap foreach {
              case (fileId, accessTime) =>
                if (System.currentTimeMillis() - accessTime.toLong > maxAccessTime)
                {
                  metaStore.removeReadActor(readActor)
                  metaStore.updateReadFileCount(fileId, -1)
                }
                else
                {
                  metaStore.updateReadFileCount(fileId, 1)
                }
            }
          }
        }
      }
    }
  }

  private def cleanInactiveFiles ()
  {
    val fileReadCount = metaStore.getReadFileCountSet()
    val smallestFileCount = fileReadCount.head

    if (smallestFileCount._2 == 0)
    {
      metaStore.rm(smallestFileCount._1) foreach {
        key => dataStore.deleteData(key)
      }

    }
  }

  private def processWriteMsg ()
  {
    val writeActorSetOpt = metaStore.getWriteActorActiveSet()

    writeActorSetOpt foreach {
      writeActorSet => writeActorSet foreach {
        writeActorOpt => writeActorOpt foreach {
          writeActor => metaStore.getWriteActorAccess(writeActor) foreach  {
            accessMap => accessMap foreach {
              case (fileId, accessTime) =>
                if (System.currentTimeMillis() - accessTime.toLong > maxAccessTime)
                {
                  metaStore.removeWriteActor(writeActor)
                  metaStore.updateWriteFileCount(fileId, -1)
                }
                else
                {
                  metaStore.updateWriteFileCount(fileId, 1)
                }
            }
          }
        }
      }
    }
  }

}

