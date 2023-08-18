package util

import java.io.{BufferedInputStream, BufferedReader, InputStreamReader}

/*
文件读取工具类
 */
object FileUtis {
  def readResourcesFile(fileName: String): BufferedReader = {
    val input = this.getClass.getClassLoader.getResourceAsStream(fileName)
    val inputStreamReader = new InputStreamReader(input, "UTF-8")
    new BufferedReader(inputStreamReader)
  }

  def main(args: Array[String]): Unit = {
    // test
    val reader = readResourcesFile("data.csv")
    println(reader.readLine())
  }

}
