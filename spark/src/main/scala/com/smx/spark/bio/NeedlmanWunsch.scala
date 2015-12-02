package com.smx.spark.bio

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object NeedlemanWunsch {
    def main(args: Array[String]) {
    //val logFile = "YOUR_SPARK_HOME/README.md" // Should be some file on your system

    val fastaFileQuery = "C:/genomes/query.fna"
    val fastaFileTarget = "C:/genomes/target.fna"
    
    val conf = new SparkConf()
        .setAppName("Bio Application")
        .setMaster("local")
        
    val sc = new SparkContext(conf)
    
    val queryRDD = sc.textFile(fastaFileQuery).partitioner
    val targetRDD = sc.textFile(fastaFileTarget, 10).flatMap(_.toCharArray())
    
    val newRDD = targetRDD.mapPartitionsWithIndex { 
      (index:Int, value:Iterator[(Char)]) => {
          value.map(compound => (index, compound))
        }    
     }

    // you can really only use the broadcast for shipping the query  
    // compound and maybe some other stuff such as the substitution matrix
    // it doesn't work as expected, i.e., you can't continually broadcast
    val broadcast = sc.broadcast(0)
    val brdcstQueryCompound = sc.broadcast(0)
    val accum = sc.accumulator(0)

    
    newRDD.foreachPartitionAsync( partitionOfTarget => { 
      
      // how do we get the index of this partition? 
      // one way to do it is to mapPartitionsWithIndexm
      val index = partitionOfTarget.take(1).toList(0)._1
      
      println("This is partition %s".format(index))
      
      
      //while (broadcast != index) {
       // continue until broadcast says this partition can go 
      //}
      
      partitionOfTarget.foreach(line => {

        // RDD Transform
        
        // line._2 is the target compound
        // line._3 is value for current
        // line._4 is the value for left which equals score for previous compound
        // line._5 is the value for corner which equals score for previous compound, previous rdd
        // line._6 is the value for top which equals score for same compound previous rdd
        
        
			 if(line._2 == brdcstQueryCompound)
			 {
				 //line._3 = line._5+1;
			 }
			 else
			 {
				 //if (line._4 > line._6)
				 {
					 //line._3 = line._4;
				 }
				 //else
				 {
					 //line._3 = line._6
				 }
			 }
        
      })
      
      accum += index+1
      
    })

    while (accum.value.toInt < (10*(10+1))/2) {
      
    }
    
    println("done " + accum.value)
  }
}