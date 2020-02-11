
  import scala.io.Source
  import scala.io.Codec
  import java.nio.charset.CodingErrorAction
  import org.apache.spark.rdd.RDD
  import org.apache.spark.{SparkConf, SparkContext}
  import scala.collection.mutable.ArrayBuffer


  object freestyle extends App
  {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    var spellArray: Array[Spell] = Array()

    for(i <- 0 to 10) { //1975
      var stringUrl = "http://www.dxcontent.com/SDB_SpellBlock.asp?SDBID=" + i
      val html = Source.fromURL(stringUrl)
      val htmlString = html.mkString

      val indexLevel = htmlString.indexOf("<B>level</b>") + "<B>level</b>".length
      val levelSorcerer = htmlString.substring(indexLevel, htmlString.indexOf("</p", indexLevel))
      //println(levelSorcerer)

        val indexName = htmlString.indexOf("heading'><P>") + "heading'><P>".length
        val nameSpell = htmlString.substring(indexName, htmlString.indexOf("<", indexName))
        //println("nameSpell= " + nameSpell)

        val indexComponent = htmlString.indexOf("<b>Components</b>") + "<b>Components</b>".length
        val components = htmlString.substring(indexComponent, htmlString.indexOf("<", indexComponent))
        //println("components= " + components)

        var isSpellResistance = false

        if(htmlString.contains("Spell Resistance")) {
          val indexSpellResistance = htmlString.indexOf("Spell Resistance") + "Spell Resistance".length
          val spellResistance = htmlString.substring(indexSpellResistance, htmlString.indexOf("</p>", indexSpellResistance))
          isSpellResistance = spellResistance.contains("yes")
          println("isSpellResistance= " + isSpellResistance)
        }
        spellArray(i) = new Spell(levelSorcerer.toString, nameSpell, components, isSpellResistance)

    }


    class Spell(var levelSorcerer: String = "", var nameSpell: String = "", var components: String  = "", var spellResistant: Boolean = false) extends Serializable {}

    val conf = new SparkConf()
      .setAppName("A spell for a wizard")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")


    /*var spellArray: Array[Spell] = Array()
      new Spell("1", "a spell", "V", true),
      new Spell("2", "an other spell", "VF", false)*/

    val resultatRDD = sc.makeRDD(spellArray)
    val pairs = resultatRDD.map(element => (element, 1))
    pairs.collect().foreach(element => println(element._1.nameSpell))




/*
    val conf = new SparkConf()
      .setAppName("Petit exemple")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")


    val tableau: Array[Int] = Array(1,2,3,4,5)

    val resultatRDD = sc.makeRDD(tableau).map( element => {
      element * 3
    }).cache()

    resultatRDD.collect().foreach(  e => print(e))

    println("*****")
    //On veut garder le plus petit de toute la collection
    val a =  resultatRDD.reduce(   (a, b) => {
      if (a < b) a else b
    })

    println(a)

    val c = resultatRDD.aggregate("")(
      (acc, nouveau) => acc + nouveau.toString,
      (a,b) => a.toString + b.toString
    )

    //Tous les éléments impairs vont avoir la catégorie 2
    //et les pairs vont avoir la catégorie (clé) 1
    //On va créer un PairRDD
    val r1: RDD[(Int, Int)] = resultatRDD.map(elem => {
      var categorie = 1
      if (elem % 2 == 1) categorie = 2
      (categorie, elem)
    })

    r1.reduceByKey(  (a , b ) =>  a+b).collect()
      .foreach( e => println(e))


    println("Resultats du flatmap")

    //3 6 9 12 15
    resultatRDD.flatMap(  elem => {
      var resultats = new ArrayBuffer[(Int,Int)]()
      for (i <- 1 to elem) {
        var categorie = 1
        if (i % 2 == 1) categorie = 2
        resultats += Tuple2(elem, i)
      }
      resultats
    }) .collect(). foreach(  e => println(e))
*/

    //
    //  println("******")
    //  print(c)
  }