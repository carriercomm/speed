package samples

import org.junit._
import Assert._
import junit.framework.TestCase
import flipkart.platform.file.FileMetaData
import flipkart.platform.cachefarm.Prometheus
import flipkart.platform.cachefarm.config.Configuration
import com.redis.RedisClient
import flipkart.platform.buffer.SpeedBufStatus
import collection.mutable.ListBuffer
import java.io._
import flipkart.platform.{LightningConfig, Lightning}

@Test
class AppTest extends TestCase
{

  val dataFile = "/Users/vivekys/Temp/diag"

  val sampleFile = "/Users/vivekys/Temp/sample"

  val lightningConfig = new LightningConfig(
          metaStoreHost = "localhost",
          metaStorePort = 6379,
          dataStoreHost = Array("pf-eng1"),
          dataStorePort = 8091,
          dataStoreBucket = "datastore",
          dataChunkSize = 409600,
          preFetchSize = 4,
          writerConcurrencyFactor = 4,
          readerConcurrencyFactor = 5,
          logFile = "/tmp/speed-lib.log"
  )

  val lightning = new Lightning(lightningConfig)


  def resetAllData() =
  {
    val redisClient = new RedisClient(lightningConfig.metaStoreHost, lightningConfig.metaStorePort)
    redisClient.flushall

    val membaseClient = new Prometheus(new Configuration(lightningConfig.dataStoreHost,
        lightningConfig.dataStoreBucket, lightningConfig.dataStoreBucket, ""))

    membaseClient.flush()
  }

  def check(value: Array[Byte], result: Array[Byte]): Unit =
  {
    assertEquals(value.length, result.length)
    for (i <- 0 to value.length - 1)
      assertEquals(value(i), result(i))
  }

  @Test
  def testCreate()
  {
    resetAllData()
    assertTrue(lightning.create("sample"))
  }

  @Test
  def testDuplicateCreate()
  {
    assertFalse(lightning.create("sample"))
  }

  @Test
  def testReadBeforeWrite()
  {
    try
    {
      lightning.read("sample")
    } catch
    {
      case ex : IOException => assertTrue(true)
    }
  }

  @Test
  def testWriteFile()
  {
    val file = new File(sampleFile)
    val fin = new FileInputStream(file);

    lightning.write("sample", new FileMetaData("sample", file.length()), fin)
  }

  def inputStreamToByteArray(is: InputStream): Array[Byte] =
    Iterator continually is.read takeWhile (-1 !=) map (_.toByte) toArray

  def testReadFile()
  {
    val file = new File(sampleFile)
    val fin = new FileInputStream(file);
    val data = inputStreamToByteArray(fin)
    Thread.sleep(2000)
    val buf = lightning.read("sample")

    val dataRead = ListBuffer[Byte]()


    var byteCount = file.length()

    while (byteCount > 0)
    {
      if (buf.bufReadable == SpeedBufStatus.YES)
      {
        try
        {
          Thread.sleep(100)
          dataRead.append(buf.read())
          byteCount -= 1
        }
        catch
        {
          case _ => println("Empty !!")
        }
      }
    }

    val byteArray = dataRead.toArray
    assertArrayEquals(data, byteArray)
  }

  @Test
  def testDeleteFile()
  {
    assertTrue(lightning.delete("sample"))
  }

  @Test
  def testls()
  {
    val tuples = lightning.ls()
    for (i <- tuples)
      println(i)
  }
}


