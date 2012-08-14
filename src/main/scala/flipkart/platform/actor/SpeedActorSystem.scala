package flipkart.platform.actor

import akka.actor.ActorSystem._
import akka.actor.Props._
import akka.actor.{Props, ActorSystem}

/**
 * Created by IntelliJ IDEA.
 * User: vivekys
 * Date: 14/08/12
 * Time: 4:14 PM
 * To change this template use File | Settings | File Templates.
 */

object SpeedActorSystem
{
  val system = ActorSystem("speed")
  val fileStateActor = system.actorOf(Props[FileStateManager], name = "fileStateActor")

  def getActorSystem () = system

  def getFileStateActor () = fileStateActor

}