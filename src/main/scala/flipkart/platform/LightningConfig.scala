package flipkart.platform

/**
 * Created by IntelliJ IDEA.
 * User: vivekys
 * Date: 03/08/12
 * Time: 3:06 PM
 * To change this template use File | Settings | File Templates.
 */

class LightningConfig(val metaStoreHost: String, //MetaStore host & in this case redis host
                      val metaStorePort: Int, // MetaStore port & in this case redis port
                      val dataStoreHost: Array[String], //DataStore hosts & in this case membase cluster hosts
                      val dataStorePort: Int, //DataStore port & in this case membase cluster port
                      val dataStoreBucket: String, //Membase bucket Name
                      val dataChunkSize: Int, val preFetchSize: Int,
                      val logFile: String)
{

  import com.codahale.logula.Logging
  import org.apache.log4j.Level

  Logging.configure
  {
    log =>
      log.registerWithJMX = true

      log.level = Level.ALL
      log.loggers("buffer") = Level.OFF

      log.console.enabled = true
      log.console.threshold = Level.WARN

      log.file.enabled = true
      log.file.filename = logFile
      log.file.maxSize = 10 * 10 * 1024 // 1MB
      log.file.retainedFiles = 5 // keep five old logs around

  }
}