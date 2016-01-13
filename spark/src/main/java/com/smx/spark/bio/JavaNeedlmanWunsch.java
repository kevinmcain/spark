package com.smx.spark.bio;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;

import scala.Tuple2;

public class JavaNeedlmanWunsch {	
	public static void main(String[] args) {
		  
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Spark Count"));
		 
		Function2 removeHeader= new Function2<Integer, Iterator<String>, Iterator<String>>(){
			@Override
		    public Iterator<String> call(Integer ind, Iterator<String> iterator) throws Exception {
		        if(ind==0 && iterator.hasNext()){
		            iterator.next();
		            return iterator;
		        }else
		            return iterator;
		    }
		};
		    
	    // split each document into words
	    JavaRDD inputRDD = sc.textFile(args[0]).mapPartitionsWithIndex(removeHeader, false);
	    
	    System.out.println(inputRDD.toString());
  }
	  

}
