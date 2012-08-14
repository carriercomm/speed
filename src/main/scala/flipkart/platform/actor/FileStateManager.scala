package flipkart.platform.actor

import flipkart.platform.file._
import com.codahale.logula.Logging
import com.redis.RedisClient
import akka.actor.{ActorRef, Props, ActorSystem, Actor}

/**
 * Created by IntelliJ IDEA.
 * User: vivekys
 * Date: 12/08/12
 * Time: 11:27 AM
 * To change this template use File | Settings | File Templates.
 */

case class FileStateMsg(fileName : String, version : Int,
                        fileStatus : FileStatus.Value, metaStoreHandler : RedisClient)

class FileStateManager extends Actor with Logging {
  protected def receive =
  {
    case FileStateMsg(fileName, version, fileStatus, metaStoreHandler : RedisClient) =>
      val currentState = metaStoreHandler.hget(MetaStoreUtil.schemeFileNameVersion(fileName, version),
                                              FileMapFileNameVersionFields.State.toString)
      if (currentState == fileStatus.toString)
      {
        log.info("Current State of " + MetaStoreUtil.schemeFileNameVersion(fileName, version) + " is Same as"
                      + fileStatus.toString)
      }
      else {
        metaStoreHandler.hset(MetaStoreUtil.schemeFileNameVersion(fileName, version),
                                              FileMapFileNameVersionFields.State.toString, fileStatus.toString)
        log.info("Setting State of " + MetaStoreUtil.schemeFileNameVersion(fileName, version) + " to"
                    + fileStatus.toString)
      }
      sender ! (true)
  }
}

object SpeedActorSystem
{
  val system = ActorSystem("speed")
  val fileStateActor = system.actorOf(Props[FileStateManager], name = "fileStateActor")

  def getActorSystem () = system

  def getFileStateActor () = fileStateActor

}