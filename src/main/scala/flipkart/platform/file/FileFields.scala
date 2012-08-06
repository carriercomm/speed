package flipkart.platform.file

/**
 * Created by IntelliJ IDEA.
 * User: vivekys
 * Date: 02/08/12
 * Time: 2:29 PM
 * To change this template use File | Settings | File Templates.
 */

object FileFields extends Enumeration {
  val NAME = Value("Name")
  val SIZE = Value("Size")
  val STATUS = Value("Status")
  val NUMCHUNKS = Value("NumChunks")
}