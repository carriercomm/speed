package flipkart.platform.randomGenerator

import java.util.UUID


/**
 * Created by IntelliJ IDEA.
 * User: vivekys
 * Date: 02/08/12
 * Time: 1:39 PM
 * To change this template use File | Settings | File Templates.
 */

object RandomGenerator {
  def generate () : String = {
    UUID.randomUUID().toString
  }
}