package flipkart.platform.buffer

import com.codahale.logula.Logging

/**
 * Created by IntelliJ IDEA.
 * User: vivekys
 * Date: 02/08/12
 * Time: 12:03 PM
 * To change this template use File | Settings | File Templates.
 */

trait SpeedBuf extends  Logging {
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

  def bufReadable () : SpeedBufStatus.Value

  def bufWriteComplete () : SpeedBufStatus.Value
}

object SpeedBufStatus extends Enumeration
{
  val UNKNOWN = Value("Unknown")
  val YES = Value("Yes")
  val NO = Value("No")
}