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
  def createFile (fileName: String, attr : FileMetaData) : Boolean

  //Adds a chunk ID and map it against seq number for the given file
  def addChunk(fileName: String, chunkSeq : Double, chunkId: String)

  //Set file status
  def setFileStatus(fileName: String,  status : FileStatus.Value, opCount : Int) :Boolean

  //Set file metadata
  def setFileMetaData (fileName : String, attr : FileMetaData)

  //Update the number of chunks for the given filename
  def updateFileChunkCount(fileName:String, chunkCnt:Int)

  //Get file status
  def getFileStatus(fileName:String) : FileStatus.Value

  //Get file size
  def getFileSize(fileName:String) : Int

  //list the chunks for a given file in the order
  def listChunk (fileName : String) : List[String]

  //list files
  def listFiles () : HashMap[String, String]

  //delete files
  def deleteFile (fileName : String)
}