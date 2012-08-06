package flipkart.platform.file

/**
 * Created by IntelliJ IDEA.
 * User: vivekys
 * Date: 02/08/12
 * Time: 12:13 PM
 * To change this template use File | Settings | File Templates.
 */

object FileStatus extends Enumeration{
  val WRITING = Value("Write")
  val READING = Value("Read")
  val DELETING = Value("Delete")
  val IDLE = Value("Idle")
}