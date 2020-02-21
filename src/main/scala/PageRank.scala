import java.util

import org.apache.spark.{SparkConf, SparkContext}


object PageRank extends App
{
  //Initiating Spark
  val nbPage = 4
  val conf = new SparkConf()
    .setAppName("A PageRank algorithm, for the web")
    .setMaster("local[*]")
  val sc = new org.apache.spark.SparkContext(conf)
  sc.setLogLevel("ERROR")

  //class Page to represent the nodes of the graph
  class Page(var key: String = "", var pageRank: Double = 0, var adjList: Array[String]) extends java.io.Serializable {}


  //Initiating the nodes
  var pageArray = new Array[Page](nbPage)
  pageArray(0) = new Page("A", 1, Array("B", "C"))
  pageArray(1) = new Page("B", 1, Array("C"))
  pageArray(2) = new Page("C", 1, Array("A"))
  pageArray(3) = new Page("D", 1, Array("C"))

  //function to create all the (key, value) tuples messages coming from a node for other node and itself
  val mappingFunction = (page: Page) => /*util.ArrayList[(String, Double)] =*/ {
    val list = new util.ArrayList[(String, Double)]
    var i = 0
    page.adjList.foreach(pageAdj => {
      list.add((page.adjList(i), 0.85 * (page.pageRank / page.adjList.length)))
      i+=1
    }
    )
    list.add((page.key, 1 - 0.85))
    list
  }

  //function to flatten an Array[util.ArrayList[(String, Double)]] to Array[(String, Double)]
  def myFlatten(array: Array[util.ArrayList[(String, Double)]]): Array[(String, Double)] = {
    var size = 0
    array.foreach(arrayList => size += arrayList.size())
    val resultList = new Array[(String, Double)](size)
    var i = 0
    for(j <- 0 until array.length) {
      for(k <- 0 until array(j).size()) {
        resultList(i) = array(j).get(k)
        i+=1
      }
    }
    resultList
  }

  //Iteration loop for the algorithm
  for (j <- 1 to 20) {
    println("ITERATION " + j)
    val result = sc.makeRDD(pageArray)

    val messages = myFlatten(result.collect().map(page => mappingFunction(page)))
    val iterationResult = sc.makeRDD(messages).reduceByKey((accum, value) => accum + value).collect()


    //update the values of the original array:
    var pageRankA = 0.0
    var pageRankB = 0.0
    var pageRankC = 0.0
    var pageRankD = 0.0

    iterationResult.foreach(page =>
      page._1 match {
        case "A" => pageRankA = page._2
        case "B" => pageRankB = page._2
        case "C" => pageRankC = page._2
        case "D" => pageRankD = page._2
      }
    )
    println("A = " + pageRankA)
    println("B = " + pageRankB)
    println("C = " + pageRankC)
    println("D = " + pageRankD)
    println()

    pageArray(0).pageRank = pageRankA
    pageArray(1).pageRank = pageRankB
    pageArray(2).pageRank = pageRankC
    pageArray(3).pageRank = pageRankD
  }


}