package flipkart.platform

import file.{FileStatus, FileMetaData}
import java.io.InputStream
import store.{StoreManager, MembaseStore, RedisStore}

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
    storeManager.create(fileName, metaData)
  }

  def write(fileName: String, inputStream: InputStream) = {
    storeManager.write(fileName, inputStream)
  }

  def read(fileName: String) = {
    storeManager.read(fileName)
  }

  def delete(fileName: String) = {
    storeManager.delete(fileName)
  }

  def ls() = {
    storeManager.ls()
  }
}