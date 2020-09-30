import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.{File, PrintWriter}
import play.api.libs.json._

import purecsv.safe._
import org.json4s.native.JsonMethods._
import org.json4s.JsonDSL.WithDouble._
import org.codehaus.jackson.map.ObjectMapper
import org.elasticsearch.spark.rdd.EsSpark
import org.json4s.DefaultFormats
import scala.language.dynamics

import scala.{:+, ::}

object positionalIndexing extends App {
  //configure scala
  val conf = new SparkConf().setAppName("positional Indexing").setMaster("local")
  val sc = new SparkContext(conf)
  //read files from the files folder and read stop words
  val files = (new File("files")).listFiles.filter(_.isFile).toList
  val stop_words= sc.textFile("stop_words.txt").flatMap(line=>line.split("\\\\n"))

  var fileCountglobal = 0

  //create CSV for mapping documents with their ids
  var files_array= files.map(file=> file.toString).zipWithIndex
  files_array.writeCSVToFileName("docId_filePath_mapping.csv", header=Some(Seq("Document Path", "Document Id")))

  //read files one by one, for each word return (the word, the file it occurred in, its index in that file)
  val dictionaries = files.map(file=>{
    var fileCount = fileCountglobal
    //read text file without stop words
    val textFileRdd = sc.textFile(file.toString).subtract(stop_words)
    val result = textFileRdd.flatMap(line=>line.split(" "))
      .filter(_.length() > 3 )
      .zipWithIndex
      .map{
      case(word, count) => {
        (word, fileCount, count)
      }
    }
    fileCountglobal += 1
    result
  })
  //change the structure of the records to group the occurrences by word in each document.
  //new format(word, list(file, [occurrences]) )
  val result = dictionaries.map(file =>{
    file.map{case(x, y, z) => ((x,y), List(z))}
    .reduceByKey(_++_)
    .map{case(key, list) => (key._1, Map((key._2 -> list.sorted)))}
    .reduceByKey(_++_)
  })
  //merge the first two files
  var finalResult = result(0).fullOuterJoin(result(1)).map {
    case (a, (None, Some(c))) => (a, List(c))
    case (a, (Some(b), None)) => (a, List(b))
    case (a, (Some(b), Some(c))) => (a, List(b, c))
  }
  //merge the rest of the files
  for( a <- 2 to files.length-1){
    finalResult = finalResult.fullOuterJoin(result(a)).map {
      case (a, (None, Some(c))) => (a, List(c))
      case (a, (Some(b), None)) => (a, b)
      case (a, (Some(b), Some(c))) => (a, b :+ c )
    }
  }
  //turn the result into a json object
  val json = compact(render (finalResult.collect().toList.map{(word_record) =>
      ("name", word_record._1)~
        ("documents Count", word_record._2.length)~
        ("documents", word_record._2.flatten.map(record =>
          ("docId", record._1)~
          ("indexes", record._2)
      ) )
  }))
  //export the JSON result into a file
  val writer = new PrintWriter(new File("pos_inverted_index.json"))
  writer.write(json)
  writer.close()


  //prompt user to input phrase and get the mapings for all the inputted terms
  var input_value = scala.io.StdIn.readLine().split(" ")
    .map{x=>
      finalResult.collect().toList.toMap.get(x).get.flatten
    }

  var joined = (input_value(0).flatMap{case (ka,va) => input_value(1).toMap.get(ka).map(vb => (ka, List(va,vb)))})
  for (a <- 2 to input_value.length-1){
    joined = (joined.flatMap{case (ka,va) => input_value(a).toMap.get(ka).map(vb => (ka, va :+ vb))})
  }
  joined.foreach(println)


}

