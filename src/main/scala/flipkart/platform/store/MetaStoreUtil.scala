package flipkart.platform.store

/**
 * Created by IntelliJ IDEA.
 * User: vivekys
 * Date: 12/08/12
 * Time: 11:38 AM
 * To change this template use File | Settings | File Templates.
 */

object MetaStoreUtil {
  val FILEMAP = "FILEMAP"

  val FILEDIR = "FILEDIR"

  val FILECHUNK = "FILECHUNK"

  def schemeFileNameForDir(fileName: String) = FILEMAP + fileName

  def schemeFileNameForChunks(fileName: String) = FILECHUNK + fileName

}