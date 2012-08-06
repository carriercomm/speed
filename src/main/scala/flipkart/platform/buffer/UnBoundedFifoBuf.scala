package flipkart.platform.buffer

import org.apache.commons.collections.buffer.UnboundedFifoBuffer


/**
 * Created by IntelliJ IDEA.
 * User: vivekys
 * Date: 02/08/12
 * Time: 8:07 PM
 * To change this template use File | Settings | File Templates.
 */

class UnBoundedFifoBuf extends SpeedBuf {
  val buf = new UnboundedFifoBuffer()

  def read() = buf.remove().asInstanceOf[Byte]

  def read(items: Int) : Array[Byte] = {
    val returnItems = new Array[Byte](items)
    for ( i <- 0 to items-1)
      returnItems(i) = buf.remove().asInstanceOf[Byte]

    return returnItems
  }

  def write(item: Byte) = buf.add(item)

  def write(items: Array[Byte]) = {
    for (i <- 0 to items.length - 1)
      write(items(i))
  }

}