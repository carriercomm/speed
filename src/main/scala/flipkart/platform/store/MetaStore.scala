package flipkart.platform.store

import flipkart.platform.file.{FileStatus, FileMetaData}
import com.codahale.logula.Logging
import collection.mutable.HashMap


/**
 * Created by IntelliJ IDEA.
 * User: vivekys
 * Date: 02/08/12
 * Time: 1:09 PM
 * To change this template use File | Settings | File Templates.
 */

trait MetaStore extends Logging {

  //Creates a file on MetaStore
  def createFile (fileName: String) : Boolean

  //Adds a chunk ID and map it against seq number for the given file
  def addChunk(fileName: String, version: Int, chunkSeq : Double, chunkId: String)

  //Set file status
  def setFileStatus(fileName: String, version: Int, status: FileStatus.Value) :Boolean

  //Set file metadata & returns the version
  def setFileMetaData (fileName : String, attr : FileMetaData) : Int

  //Set file current version
  def setFileCurrentVersion(fileName:String, version:Int)

  //Update the number of chunks for the given filename
  def updateFileChunkCount(fileName:String, version : Int, chunkCnt:Int)

  //Get file status
  def getFileStatus(fileName:String) : FileStatus.Value

  //Get file status
  def getFileStatus(fileName:String, version : Int) : FileStatus.Value

  //Get file size
  def getFileSize(fileName:String) : Int

  //Get the current Version of the file on MetaStore
  def getCurrentVersion(fileName: String) : Int

  //list the chunks for a given file in the order
  def listChunk (fileName : String) : List[String]

  //list files
  def listFiles () : HashMap[String, String]

  //delete files
  def deleteFile (fileName : String)

  //isExist
  def isExist(fileName : String) : Boolean
}