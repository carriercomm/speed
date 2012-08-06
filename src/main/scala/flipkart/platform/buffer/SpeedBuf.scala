package flipkart.platform.buffer

/**
 * Created by IntelliJ IDEA.
 * User: vivekys
 * Date: 02/08/12
 * Time: 12:03 PM
 * To change this template use File | Settings | File Templates.
 */

trait SpeedBuf {
  /**
   * returns a byte from the buffer
   */
  def read () :Byte

  /**
   * returns an array of bytes whose length is items
    */
  def read (items : Int) : Array[Byte]

  /**
   * Writes an item of type byte to Buffer
   */
  def write (item : Byte)

  /**
   * Writes an sequence of items of type byte to Buffer
   */
  def write (items : Array[Byte])
}