package flipkart.platform.store

import flipkart.platform.file.{FileFields, FileStatus}
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

case class FileStateMsg(fileName: String, status: FileStatus.Value, opCount: Int, redisHandler : RedisClient)
case class FileStateMsgWithRef(fileName: String, status: FileStatus.Value, opCount: Int, redisHandler : RedisClient, parent : ActorRef)
case class FileStateResult(fileName:String,  status: FileStatus.Value)

class FileStateManager extends Actor with Logging {
  protected def receive =
  {
    case FileStateMsg(fileName, FileStatus.IDLE, opCount, redisHandler) if opCount == 0 =>
      redisHandler.hset(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.STATUS.toString,
          FileStatus.IDLE.toString)
      redisHandler.hset(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.OPCNT.toString, 0.toString)

      SpeedActorSystem.getActorSystem().eventStream.publish(FileStateResult(fileName, FileStatus.IDLE))
      log.info("Setting File Status " + fileName + " to " + FileStatus.IDLE.toString)
      sender ! (true)

    case FileStateMsgWithRef(fileName, FileStatus.IDLE, opCount, redisHandler, parent) if opCount == 0 =>
      redisHandler.hset(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.STATUS.toString,
          FileStatus.IDLE.toString)
      redisHandler.hset(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.OPCNT.toString, 0.toString)

      SpeedActorSystem.getActorSystem().eventStream.publish(FileStateResult(fileName, FileStatus.IDLE))
      log.info("Setting File Status " + fileName + " to " + FileStatus.IDLE.toString)
      parent ! (true)

    case FileStateMsg(fileName, FileStatus.WRITING, opCount, redisHandler) if opCount == -1 =>
      val opCnt = redisHandler.hget(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.OPCNT.toString) match
      {
        case Some(x) => x.toInt
        case None => -1
      }
      val op = redisHandler.hget(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.STATUS.toString) match
      {
        case Some(x) => x
        case None => null
      }

      if (opCnt == 1 &&
        op == FileStatus.WRITING.toString)
        self ! FileStateMsgWithRef(fileName, FileStatus.IDLE, 0, redisHandler, sender)
      else
      {
        log.error("SetFileStatus -- FileName : " + fileName + " FileStatus : " + FileStatus.WRITING.toString
          + " OpCnt : " + opCount.toString + "Current Status : " + op)
        sender ! (false)
      }

    case FileStateMsg(fileName, FileStatus.WRITING, opCount, redisHandler) if opCount == 1 =>
      val opCnt = redisHandler.hget(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.OPCNT.toString) match
      {
        case Some(x) => x.toInt
        case None => -1
      }
      val op = redisHandler.hget(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.STATUS.toString) match
      {
        case Some(x) => x
        case None => null
      }
      if ((op == FileStatus.IDLE.toString || op == FileStatus.RW.toString))
      {
        if (opCnt == 0)
        {
          redisHandler.hset(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.STATUS.toString, FileStatus.WRITING.toString)
          redisHandler.hset(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.OPCNT.toString, (opCnt + 1).toString)
          SpeedActorSystem.getActorSystem().eventStream.publish(FileStateResult(fileName, FileStatus.WRITING))
          log.info("Setting File Status " + fileName + " to " + FileStatus.WRITING.toString)
          sender ! (true)
        }
        else
        {
          log.error("SetFileStatus -- FileName : " + fileName + " FileStatus : " + FileStatus.WRITING.toString
            + " OpCnt : " + opCount.toString + " Current Status : " + op)
          sender ! (false)
        }
      }
      else if (op == FileStatus.READING)
        self ! FileStateMsgWithRef(fileName, FileStatus.RW, 1, redisHandler, sender)
      else
      {
        log.error("SetFileStatus -- FileName : " + fileName + " FileStatus : " + FileStatus.WRITING.toString
          + " OpCnt : " + opCount.toString + "Current Status : " + op)
        sender ! (false)
      }

    case FileStateMsg(fileName, FileStatus.READING, opCount, redisHandler) if opCount == -1 =>
      val opCnt = redisHandler.hget(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.OPCNT.toString) match
      {
        case Some(x) => x.toInt
        case None => -1
      }
      val op = redisHandler.hget(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.STATUS.toString) match
      {
        case Some(x) => x
        case None => null
      }

      if (op == FileStatus.READING.toString)
      {
        if (opCnt == 1)
          self ! FileStateMsgWithRef(fileName, FileStatus.IDLE, 0, redisHandler, sender)
        else
        {
          redisHandler.hset(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.STATUS.toString, FileStatus.READING.toString)
          redisHandler.hset(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.OPCNT.toString, (opCnt - 1).toString)
          log.info("Setting File Status " + fileName + " to Reading with decremented opCnt")
          sender ! (true)
        }
      }
      else if (op == FileStatus.RW.toString)
      {
        if (opCnt == 1)
        {
          redisHandler.hset(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.OPCNT.toString, (opCnt - 1).toString)
          self ! FileStateMsgWithRef(fileName, FileStatus.WRITING, 1, redisHandler, sender)
        }
        else
        {
          redisHandler.hset(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.STATUS.toString, FileStatus.READING.toString)
          redisHandler.hset(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.OPCNT.toString, (opCnt - 1).toString)
          log.info("Setting File Status " + fileName + " to Reading with decremented opCnt")
          sender ! (true)
        }
      }
      else
      {
        log.error("SetFileStatus -- FileName : " + fileName + " FileStatus : " + FileStatus.READING.toString
          + " OpCnt : " + opCount.toString + "Current Status : " + op)
        sender ! (false)
      }

    case FileStateMsg(fileName, FileStatus.READING, opCount, redisHandler) if opCount == 1 =>
      val opCnt = redisHandler.hget(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.OPCNT.toString) match
      {
        case Some(x) => x.toInt
        case None => -1
      }
      val op = redisHandler.hget(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.STATUS.toString) match
      {
        case Some(x) => x
        case None => null
      }

      if (op == FileStatus.IDLE.toString ||
        op == FileStatus.READING.toString)
      {
        redisHandler.hset(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.STATUS.toString,
            FileStatus.READING.toString)
        redisHandler.hset(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.OPCNT.toString, (opCnt +
              1).toString)

        SpeedActorSystem.getActorSystem().eventStream.publish(FileStateResult(fileName, FileStatus.READING))
        log.info("Setting File Status " + fileName + " to " + FileStatus.READING.toString)
        sender ! (true)
      }
      else
      {
        log.error("SetFileStatus -- FileName : " + fileName + " FileStatus : " + FileStatus.READING.toString
          + " OpCnt : " + opCount.toString + "Current Status : " + op)
        sender ! (false)
      }

    case FileStateMsg(fileName, FileStatus.RW, opCount, redisHandler) if opCount == 1 =>
      val opCnt = redisHandler.hget(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.OPCNT.toString) match
      {
        case Some(x) => x.toInt
        case None => -1
      }
      val op = redisHandler.hget(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.STATUS.toString) match
      {
        case Some(x) => x
        case None => null
      }

      if (op == FileStatus.READING.toString)
      {
        redisHandler.hset(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.STATUS.toString, FileStatus.READING.toString)

        SpeedActorSystem.getActorSystem().eventStream.publish(FileStateResult(fileName, FileStatus.RW))
        log.info("Setting File Status " + fileName + " to " + FileStatus.RW.toString)
        sender ! (true)
      }
      else
      {
        log.error("SetFileStatus -- FileName : " + fileName + " FileStatus : " + FileStatus.READING.toString
          + " OpCnt : " + opCount.toString + "Current Status : " + op)
        sender ! (false)
      }

    case FileStateMsg(fileName, FileStatus.DELETING, opCount, redisHandler) if opCount == -1 =>
      val opCnt = redisHandler.hget(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.OPCNT.toString) match
      {
        case Some(x) => x.toInt
        case None => -1
      }
      val op = redisHandler.hget(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.STATUS.toString) match
      {
        case Some(x) => x
        case None => null
      }

      if (opCnt == 1 &&
        op == FileStatus.DELETING.toString)
        self ! FileStateMsgWithRef(fileName, FileStatus.IDLE, 0, redisHandler, sender)
      else
      {
        log.error("SetFileStatus -- FileName : " + fileName + " FileStatus : " + FileStatus.DELETING.toString
          + " OpCnt : " + opCount.toString + "Current Status : " + op)
        sender ! (false)
      }

    case FileStateMsg(fileName, FileStatus.DELETING, opCount, redisHandler) if opCount == 1 =>
      val opCnt = redisHandler.hget(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.OPCNT.toString) match
      {
        case Some(x) => x.toInt
        case None => -1
      }
      val op = redisHandler.hget(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.STATUS.toString) match
      {
        case Some(x) => x
        case None => null
      }

      if (op == FileStatus.IDLE.toString)
      {
        redisHandler.hset(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.STATUS.toString, FileStatus.DELETING.toString)
        redisHandler.hset(MetaStoreUtil.schemeFileNameForDir(fileName), FileFields.OPCNT.toString, (opCnt + 1).toString)

        SpeedActorSystem.getActorSystem().eventStream.publish(FileStateResult(fileName, FileStatus.DELETING))
        log.info("Setting File Status " + fileName + " to Deleting")
        sender ! (true)
      }
      else
      {
        log.warn("SetFileStatus -- FileName : " + fileName + " FileStatus : " + FileStatus.DELETING.toString
          + " OpCnt : " + opCount.toString + "Current Status : " + op)
        sender ! (false)
      }
  }
}

object SpeedActorSystem
{
  val system = ActorSystem("speed")
  val fileStateActor = system.actorOf(Props[FileStateManager], name = "fileStateActor")

  def getActorSystem () = system

  def getFileStateActor () = fileStateActor

}