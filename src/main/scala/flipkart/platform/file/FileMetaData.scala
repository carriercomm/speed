package flipkart.platform.file

/**
 * Created by IntelliJ IDEA.
 * User: vivekys
 * Date: 02/08/12
 * Time: 2:29 PM
 * To change this template use File | Settings | File Templates.
 */

/**
 * This file describes the layout of MetaStore
 *
 * FileDir --- Redis Set
 ****** Files in the System
 *
 * FileMap+fileName  --- Redis Hashes
 * ****** Name -- Name of the file
 * ****** Size -- Size of the file
 * ****** currentVersion -- Version of the file that can be read
 * ****** version --- Version of the file that is being created/written
 *
 * FileMap+versionSet+fileName --- Redis Sorted Set
 ****** Versions for the file in the order
 *
 * FileMap+fileName+version --- Redis Hashes
 ****** Version
 ****** State
 ****** NumChunks
 *
 * FileChunk+fileName+version --- Redis Sorted
 ****** chunks
 */

object FileMapFileNameFields extends Enumeration {
  val Name = Value("Name")
  val Size = Value("Size")
  val Version = Value("Version")
  val CurrentVersion = Value("CurrentVersion")
}

object FileMapFileNameVersionFields extends Enumeration {
  val Version = Value("Version")
  val State = Value("State")
  val NumChunks = Value("NumChunks")
}

object MetaStoreUtil {
  def schemeFileDir () = "FileDir"

  def schemeFileMap (fileName : String) = "FileMap"+fileName

  def schemeFileMapVersionSet (fileName : String) = "FileMapVersionSet"+fileName

  def schemeFileNameVersion (fileName : String,  version : Int) = "FileMap"+fileName+version

  def schemeFileChunk(fileName :String, version : Int) = "FileChunk"+fileName+version
}

object FileStatus extends Enumeration{
  val Active = Value("Active")
  val InActive = Value("InActive")
  val UnKnown = Value("Unknown")
}

class FileMetaData (val fileName:String, val size : Long){

  override def toString() : String = {
    "Name :" + fileName + " Size : " + size
  }
}