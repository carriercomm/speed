package flipkart.platform.store

import flipkart.platform.cachefarm.config.Configuration
import java.util.concurrent.TimeUnit
import flipkart.platform.cachefarm.CacheFactory
import scala.collection.JavaConversions._

/**
 * Created by IntelliJ IDEA.
 * User: vivekys
 * Date: 03/08/12
 * Time: 1:29 PM
 * To change this template use File | Settings | File Templates.
 */

class MembaseStore(val hosts : Array[String], val bucket : String) extends DataStore{
  val config: Configuration = new Configuration.ConfigBuilder(hosts, bucket, bucket, "").
          withConnPoolSize(10).withCompressTh(512).withOperationTimeOut(1, TimeUnit.SECONDS).build

  val dataStore = CacheFactory.newCache(config)

  log.info("Created Membase store " + hosts.toString + "Bucket :" + bucket)

  def addData(key: String, value: Array[Byte]) = {
    val futureResult = dataStore.add(key, value)

    val result = futureResult.get()

    log.debug("Added data with key " + key + " with result of the operation " + result)
    result
  }

  def getData(key: String) = {
    dataStore.get(key)
  }

  def asyncGet(key: String) = {
    dataStore.asyncGet(key)
  }

  def multiGetData(keys: Array[String]) = {
    mapAsScalaMap(dataStore.mget(keys.toSeq))
  }

  def deleteData(key: String) = {
    dataStore.remove(key)
  }

  def deleteData(keys: Array[String]) = {
    dataStore.mremove(keys.toSeq)
  }
}