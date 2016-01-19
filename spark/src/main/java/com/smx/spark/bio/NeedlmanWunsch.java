package com.smx.spark.bio;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;

import scala.Tuple2;

public class NeedlmanWunsch {
	
	static Logger logger = Logger.getLogger(NeedlmanWunsch.class.getName());
	
	// program arguments: src/main/resources/sequencePairs.txt
	public static void main(String[] args) {
		
		logger.info("have entered the main program");
		
//   for deployment		
//		JavaSparkContext sc = new JavaSparkContext(new SparkConf()
//        	.setAppName("Bio Application"));
		
		JavaSparkContext sc = new JavaSparkContext(new SparkConf()
    		.setAppName("Bio Application")
        	.setMaster("local[1]"));
		
		JavaRDD<String> inputRDD = sc.textFile(args[0]); // partition to number of seq pairs
		//JavaRDD<String> inputRDD = sc.textFile("src/main/resources/sequencePairs.txt",3); // partition to number of seq pairs
		
		JavaPairRDD<Integer, String> rdd = inputRDD
				.mapToPair((s) ->  {
					Integer i = Integer.parseInt(s.substring(0, 1));
					
					return new Tuple2<Integer, String>(i, s.substring(2));
				})
				.partitionBy(new CustomPartitioner(3,3));
		
//		rdd.foreach(line -> { 
//			System.out.println(line);	
//		});
		
		rdd.foreachPartition(node -> { 
    	
			// TODO: port all alignment code 
			
		 	int[][][] scores;
	        int[] dim = {31,31,3}; // length of query, length of target, 3
	
	        scores = new int[dim[0]][][];
	        scores[0] = new int[dim[1]][dim[2]];
	        scores[1] = new int[dim[1]][dim[2]];
		
	        
			//File file = new File("output/"+dnaSeq.getAccession());
			//FastaWriterHelper.writeSequence(file, dnaSeq);
	        

	    	System.out.println(node.next());
    	
		});
		
		
//		JavaRDD<Tuple2<Integer, String>> sequencePairs = inputRDD.mapPartitionsWithIndex(
//	            new Function2<Integer, Iterator<String>, Iterator<Tuple2<Integer, String>>>() {
//
//	                /**
//					 * 
//					 */
//					private static final long serialVersionUID = 1L;
//
//					public Iterator<Tuple2<Integer, String>> call(Integer k, Iterator<String> v)
//	                    throws Exception {
//						
//						List<Tuple2<Integer, String>> list = new ArrayList<Tuple2<Integer, String>>();
//						while (v.hasNext())
//							list.add(new Tuple2<Integer, String>(k, v.next()));
//
//						return list.iterator();
//	                    //return new Iterator<Tuple2<Integer, String>>(partitionId, strings);
//	                }
//	            },
//        true);
	    
	}
}

class CustomPartitioner extends Partitioner {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Integer partitions;
	Integer elements;
	
	public CustomPartitioner(Integer partitions, Integer elements) {
		this.partitions = partitions;
		this.elements = elements;
	}
	
	@Override
	public int getPartition(Object arg0) {
		Integer k = (Integer)arg0;
		return k * partitions / elements;
	}

	@Override
	public int numPartitions() {
		return partitions;
	}
	
}

// spark-nndescent/src/main/java/info/debatty/spark/nndescent/ExampleStringCosineSimilarity.java

class MyIterator implements Iterator<Node> {

    int i = 0;
    private final Integer partitionId;
    private final Iterator<String> strings;

    public MyIterator(Integer partitionId, Iterator<String> strings) {
        this.partitionId = partitionId;
        this.strings = strings;
    }

    public boolean hasNext() {
        return strings.hasNext();
    }

    public Node next() {
        return new Node("" + partitionId + ":" + i++, strings.next());
    }

    public void remove() { 
        strings.remove();
    }
}

// java-graphs/src/main/java/info/debatty/java/graphs/Node.java
/**
 * The nodes of a graph have an id (String) and a value (type T)
 * @author Thibault Debatty
 * @param <T> Type of value field
 */
class Node<T> implements Serializable {
    public String id = "";
    public T value;
    
    public Node() {
    }
    
    public Node(String id) {
        this.id = id;
    }
    
    public Node(String id, T value) {
        this.id = id;
        this.value = value;
    }

    @Override
    public String toString() {
        
        String v = "";
        if (this.value != null) {
            v = this.value.toString();
        }
            
        return "(" + id + " => " + v + ")";
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        
        if (other == null) {
            return false;
        }
        
        if (! other.getClass().isInstance(this)) {
            return false;
        }
        
        return this.id.equals(((Node) other).id);
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 83 * hash + (this.id != null ? this.id.hashCode() : 0);
        return hash;
    }
    
}


// Simple word count program
//JavaPairRDD<String, Integer> counts = inputRDD
//.flatMap(x -> Arrays.asList(x.split(" ") ) )
//.mapToPair(x -> new Tuple2<String, Integer>(x, 1) )
//.reduceByKey( (x, y) -> x + y );

//JavaRDD<String> sequencePairs = inputRDD.
//mapPartitionsWithIndex((index, value) -> {
//    if(index==0 && value.hasNext()){
//        value.next();
//        return value;
//    }else
//        return value;
//}, false);