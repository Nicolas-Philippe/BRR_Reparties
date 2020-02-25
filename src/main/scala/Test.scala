
  import scala.io.Source
  import scala.io.Codec
  import java.nio.charset.CodingErrorAction

  import org.apache.spark.rdd.RDD
  import org.apache.spark.{SparkConf, SparkContext}

  import scala.collection.mutable.ArrayBuffer
  import org.apache.spark.sql.{Row, SparkSession}
  import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType}
  import org.apache.spark.sql.DataFrame




  object freestyle extends App
  {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    
    // La variable "size" détermine le nombre de spells , correspond à l'id du dernier spell pris en compte
    // sa vleur maximal est de 1975, le nombre total de spell sur le site de référence
    var size = 1975 //1975
    // La variable spellArray correspond à la liste de spells étudiés
    var spellArray = new Array[Spell](size)

    /*
    // le code suivant n'a été exécuté qu'une seule fois pour télécharger tout les spells
    // Nous avons fait ça car le site était souvent inacessible et ce n'était donc pas pratique 
    // pour tester le programme.
    // Ces fichier sont déjà présent dans le dossier donc il n'y a pas besoin de refaire tourner
    // Cette boucle
        for(i <- 1 to size) { //1975
          var stringUrl = "http://www.dxcontent.com/SDB_SpellBlock.asp?SDBID=" + i
          val html = Source.fromURL(stringUrl)
          val htmlString = html.mkString
          reflect.io.File("Spells/" + i).writeAll(htmlString)
        }

    */


    // Cette boucle sert à remplir la liste de spell "spellArray" avec les variables correspondantes
    for(i <- 1 to size) { //1975

      // Le Code source des pages de description de tous les spells sont enregistrés dans des fichiers joints
      // ils sont récupérés sous forme de string et les informations qui nous interressent sont ensuite
      // enregistrés dans des variables ("levelSorcerer", "nameSpell", "components" et "isSpellResistance") 
      // avant d'être utilisés pour créer un nouveau spell qui sera intégré à la variable "spellArray"
      
      val html = Source.fromFile("Spells/" + i).getLines()
      val htmlString = html.mkString

      if(htmlString.contains("Level")) {   // cette condition est nécessaire puisque nous avons observés
        // que certaines pages du site étaient vierge.
        val indexLevel = htmlString.indexOf("<B>Level</b>") + "<B>Level</b>".length
        val levelSorcerer = htmlString.substring(indexLevel, htmlString.indexOf("</p", indexLevel))

        val indexName = htmlString.indexOf("heading'><P>") + "heading'><P>".length
        val nameSpell = htmlString.substring(indexName, htmlString.indexOf("<", indexName))

        val indexComponent = htmlString.indexOf("<b>Components</b>") + "<b>Components</b>".length
        val components = htmlString.substring(indexComponent, htmlString.indexOf("<", indexComponent))

        var isSpellResistance = false

        if (htmlString.contains("Spell Resistance")) {
          val indexSpellResistance = htmlString.indexOf("Spell Resistance") + "Spell Resistance".length
          val spellResistance = htmlString.substring(indexSpellResistance, htmlString.indexOf("</p>", indexSpellResistance))
          isSpellResistance = spellResistance.contains("yes")
        }
        // on intègre le spell à "spellArray"
        spellArray(i - 1) = new Spell(levelSorcerer.toString, nameSpell, components, isSpellResistance)
      }else {  // si la page observée est vierge, les champs sont remplacés par des valeurs par défaults
        spellArray(i - 1) = new Spell("Error", "Error", "Error", false)
      }
      println(i)
    }

    // La classe Spell est utilisée pour conserver les données nous intérressant pour chaque spell :
    // le niveau du spell, son nom, ses composants et la résistance.
    class Spell(var levelSorcerer: String = "", var nameSpell: String = "", var components: String  = "", var spellResistant: Boolean = false) extends Serializable {}


    val conf = new SparkConf()
      .setAppName("A spell for a wizard")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val resultatRDD = sc.makeRDD(spellArray) //met les data dans la rdd


    //  3) On filtre les spells avec les critères qui nous interressent
    println(("\nAvec RDD\n"))
     resultatRDD.filter(element => element.components.equals(" V"))
      .filter(element => element.levelSorcerer.contains("sorcerer"))
      .filter(element => {
        val indexLevel = element.levelSorcerer.indexOf("sorcerer/wizard ") + "sorcerer/wizard ".length
        val level = element.levelSorcerer.substring(indexLevel, indexLevel + 1)
        level == "1" || level == "2" || level == "3" || level == "4"
      }).foreach(element => println(element.nameSpell))


    // 4) on effetue la même opération avec une base de données relationnelle et avec le langage de requêtes SQL
    val sqlContext = SparkSession
      .builder()
      .appName("sessionBD")
      .getOrCreate()
    val schema = StructType(Array(
      StructField("levelSorcere", StringType),
      StructField("nameSpell", StringType),
      StructField("components", StringType),
      StructField("spellResistance", BooleanType)
    ))

    val rowRDD = resultatRDD.map(element => Row(element.levelSorcerer,
                                element.nameSpell, element.components,
                                element.spellResistant))

    val dataFrame = sqlContext.createDataFrame(rowRDD,schema)

    dataFrame.createTempView("allSpell")

    //println("DataFrame")
   // dataFrame.show()

   val requete = sqlContext.sql("SELECT nameSpell FROM allSpell WHERE components = ' V' AND " +
     "( levelSorcere  LIKE '%sorcerer/wizard 1%' OR levelSorcere  LIKE '%sorcerer/wizard 2%' OR levelSorcere  LIKE '%sorcerer/wizard 3%' OR levelSorcere  LIKE '%sorcerer/wizard 4%') ")
    //val nbResult = requete.count()

    println(("\nAvec SQL : \n"))
    requete.collect.foreach(println)
  }
