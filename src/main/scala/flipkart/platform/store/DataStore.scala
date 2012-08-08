package flipkart.platform.store

import java.util.concurrent.Future
import com.codahale.logula.Logging

/**
 * Created by IntelliJ IDEA.
 * User: vivekys
 * Date: 02/08/12
 * Time: 1:15 PM
 * To change this template use File | Settings | File Templates.
 */

trait DataStore extends Logging {
  /**
   * Adds data mapped against key on Data store. It will fail if key already exist
   */
  def addData (key : String, value : Array[Byte]) : Boolean

  /**
   * Get data from data store wrt to key. This is blocking call
   */
  def getData (key : String) : Array[Byte]

  /**
   * Get data from data store wrt key. This is blocking call
   */
  def asyncGet (key : String) : Future[Array[Byte]]

  /**
   * Get data for all the keys
   */
  def multiGetData (keys : Array[String]) : scala.collection.mutable.Map[String, Array[Byte]]

  // Delete data associated with the key
  def deleteData(key : String)

  //Deletes data associated with all the keys
  def deleteData(keys : Array[String])
}