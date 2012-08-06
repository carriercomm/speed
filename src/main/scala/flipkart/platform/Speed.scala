package flipkart.platform

import buffer.SpeedBuf
import file.{FileStatus, FileMetaData}
import java.io.InputStream


/**
 * Created by IntelliJ IDEA.
 * User: vivekys
 * Date: 02/08/12
 * Time: 11:56 AM
 * To change this template use File | Settings | File Templates.
 */

trait Speed {
  /**
   * Creates a new file on the store.
   * Returns true if creation succeeds
   */
  def create(fileName : String, metaData : FileMetaData) : Boolean

  /**
   * Writes the inputStream as the content of the file.
   */
  def write(fileName : String,  inputStream : InputStream)

  /**
   * If the file is readable then returns a stream buffer from which complete
   * content of the file can be read
   */
  def read(fileName : String) : SpeedBuf

  /**
   *  Not supported at this time
   */
  def update (fileName : String,  pos : Int) = {
    throw new UnsupportedOperationException("Update operation not supported in this release")
  }

  /**
   * Delete the file from the store. This will succeed only if file is
   * in idle state
   */
  def delete (fileName : String) : Boolean

  /**
   * Returns the list of file along with its state from the store
   */
  def ls () : Array[Pair[String,  FileStatus.Value]]
}