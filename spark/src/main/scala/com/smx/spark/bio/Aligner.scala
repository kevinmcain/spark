package com.smx.spark.bio

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.io.Source

object Aligner {
    def main(args: Array[String]) {
    //val logFile = "YOUR_SPARK_HOME/README.md" // Should be some file on your system

    //val sequencePairs = "C:/genomes/sequencePairs.txt"
    val sequencePairs = "src/main/resources/sequencePairs.txt"
    val fastaFileQuery = "C:/genomes/query.fna"
    val fastaFileTarget = "C:/genomes/target.fna"
    
    val conf = new SparkConf()
        .setAppName("Bio Application")
        .setMaster("local")
        
    val sc = new SparkContext(conf)
    
    val seqRDD = sc.textFile(sequencePairs, 3)
    
    val targetRDD = sc.textFile(sequencePairs, 3)
    
      val newRDD = targetRDD.mapPartitionsWithIndex { 
        (index:Int, value:Iterator[(String)]) => {
            value.map(pair => (index, pair))
          }    
       }
    
    newRDD.foreachPartition(record => { record.foreach(println)})
    

    // you can really only use the broadcast for shipping the query  
    // compound and maybe some other stuff such as the substitution matrix
    // it doesn't work as expected, i.e., you can't continually broadcast
    //val broadcast = sc.broadcast(0)
    //val brdcstQueryCompound = sc.broadcast(0)
    //val accum = sc.accumulator(0)
    
    //val rdd = newRDD
    
    seqRDD.foreachPartition( sequencePair => { 

      var i=0
      
      sequencePair.foreach { pair => 
        i+=1
        val sequenceList = pair.split(",") 
        val queryLocation = sequenceList(0)
        val targetLocation = sequenceList(1)
        
        println(queryLocation)
        println(targetLocation)
        
        val lines = Source.fromFile(queryLocation).getLines //.flatMap(_.toCharArray())
        
        lines.drop(1)
        
        lines.foreach(c => {
          println(c)
        })
        
        println(i)
        

      }
      
      println("done with pair")
      // 1. open jdbc connection
      // 2. poll database for the completion of dependent partition
      // 3. read dependent value from computed dependent partition
      // 4. compute this partition
      // 5. write this edge case result to database
      // 6. close connection
    })


  }
}