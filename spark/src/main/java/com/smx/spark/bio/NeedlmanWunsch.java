package com.smx.spark.bio;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;

import scala.Tuple2;

public class NeedlmanWunsch {
	public static void main(String[] args) {
		
		JavaSparkContext sc = new JavaSparkContext(new SparkConf()
        .setAppName("Bio Application")
        .setMaster("local"));
		
		JavaRDD<String> inputRDD = sc.textFile(args[0], 3);
		
	    // split each document into words
		JavaRDD<String> sequencePairs = inputRDD.
	    		mapPartitionsWithIndex((index, value) -> {
			        if(index==0 && value.hasNext()){
			            value.next();
			            return value;
			        }else
			            return value;
			}, false);
	    
	    
		sequencePairs.foreachPartition( line -> { 
	    	
	    	System.out.println(line.next());
	    	
    	});
	    
	}
}