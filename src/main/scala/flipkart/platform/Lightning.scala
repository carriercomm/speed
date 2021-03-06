package flipkart.platform

import file.FileMetaData
import store.StoreManager
import java.io.InputStream

/**
 * Created by IntelliJ IDEA.
 * User: vivekys
 * Date: 03/08/12
 * Time: 3:04 PM
 * To change this template use File | Settings | File Templates.
 */

class Lightning (val config : LightningConfig) extends Speed {

  val storeManager = new StoreManager(config)

  def create(fileName: String, metaData: FileMetaData) = {
    log.info("Request received to create File " + fileName + " with metadata " + metaData)
    storeManager.create(fileName, metaData)
  }

  def write(fileName: String, inputStream: InputStream) = {
    log.info("Request received for writing to file " + fileName)
    storeManager.write(fileName, inputStream)
  }

  def read(fileName: String) = {
    log.info("Request received for reading from file " + fileName)
    storeManager.read(fileName)
  }

  def delete(fileName: String) = {
    log.info("Request received for deleting file " + fileName)
    storeManager.delete(fileName)
  }

  def ls() = {
    log.info("Request received for listing files")
    val returnval = storeManager.ls()
    returnval
  }
}