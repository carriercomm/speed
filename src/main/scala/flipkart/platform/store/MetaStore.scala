package flipkart.platform.store

import flipkart.platform.file.{FileStatus, FileMetaData}
import collection.parallel.mutable


/**
 * Created by IntelliJ IDEA.
 * User: vivekys
 * Date: 02/08/12
 * Time: 1:09 PM
 * To change this template use File | Settings | File Templates.
 */

trait MetaStore {
  def createFile (fileName: String, attr : FileMetaData) : Boolean

  def addChunk(fileName: String, chunkSeq : Double, chunkId: String)

  def addChunk(fileName: String, scoreMembers : Map[java.lang.Double, String])

  def setFileStatus(fileName: String,  status : FileStatus.Value)

  def setFileMetaData (fileName : String, attr : FileMetaData)

  def getFileStatus(fileName:String) : FileStatus.Value

  def getFileSize(fileName:String) : Int

  def listChunk (fileName : String) : scala.collection.mutable.Set[String]

  def listFiles () : Map[String, String]

  def deleteFile (fileName : String) : Boolean
}