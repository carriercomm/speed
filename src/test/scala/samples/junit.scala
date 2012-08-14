package samples

import org.junit._
import Assert._
import junit.framework.TestCase
import flipkart.platform.{Lightning, LightningConfig}
import flipkart.platform.file.FileMetaData
import flipkart.platform.cachefarm.Prometheus
import flipkart.platform.cachefarm.config.Configuration
import com.redis.RedisClient
import java.nio.BufferUnderflowException
import flipkart.platform.buffer.SpeedBufStatus
import collection.mutable.ListBuffer
import akka.actor.Actors
import actors.Actor
import rules.ExpectedException
import java.io._

@Test
class AppTest extends TestCase
{

  val host = "localhost"

  val port = 6379

  val bucket = "datastore"

  val dataChunkSize = 7

  val preFetchSize = 3

  val logFile = "/tmp/SpeedTest.log"

  val dataFile = "/Users/vivekys/Temp/diag"

  val sampleFile = "/Users/vivekys/Temp/sample"

  val lightningConfig = new LightningConfig(host, port, Array("pf-eng1"), 8091,
    bucket, dataChunkSize, preFetchSize, 100, 100, logFile)

  val lightning = new Lightning(lightningConfig)

  def resetAllData() =
  {
    val redisClient = new RedisClient(host, port)
    redisClient.flushall

    val membaseClient = new Prometheus(new Configuration("pf-eng1", bucket, bucket, ""))
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
    val file = new File(sampleFile)
    assertTrue(lightning.create("sample"))
  }

  @Test
  def testDuplicateCreate()
  {
    val file = new File(sampleFile)
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
    Thread.sleep(1000)
    val buf = lightning.read("sample")

    val dataRead = ListBuffer[Byte]()


    var byteCount = file.length()

    while (byteCount > 0)
    {
      if (buf.bufReadable() == SpeedBufStatus.YES)
      {
        try
        {
          Thread.sleep(10)
          dataRead.append(buf.read())
          byteCount -= 1
        }
        catch
        {
          case e: BufferUnderflowException => "Empty !!"
                  Thread.sleep(10)
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


