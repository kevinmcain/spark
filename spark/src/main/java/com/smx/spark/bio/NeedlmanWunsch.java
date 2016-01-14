package com.smx.spark.bio;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;

import scala.Tuple2;

public class NeedlmanWunsch {
	// program arguments: src/main/resources/sequencePairs.txt
	public static void main(String[] args) {
		
		JavaSparkContext sc = new JavaSparkContext(new SparkConf()
        .setAppName("Bio Application")
        .setMaster("local"));
		
		JavaRDD<String> inputRDD = sc.textFile(args[0], 4); // partition to number of seq pairs
		
		inputRDD.foreach(line -> { 
			System.out.println(line);	
		});
		
		JavaRDD<Node> sequencePairs = inputRDD.mapPartitionsWithIndex(
	            new Function2<Integer, Iterator<String>, Iterator<Node>>() {

	                /**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					public Iterator<Node> call(Integer partitionId, Iterator<String> strings)
	                    throws Exception {
	                    return new MyIterator(partitionId, strings);
	                }
	            },
        false);
	    
		sequencePairs.foreachPartition(node -> { 
	    	
			// TODO: port all alignment code 
			
		 	int[][][] scores;
	        int[] dim = {31,31,3}; // length of query, length of target, 3

            scores = new int[dim[0]][][];
            scores[0] = new int[dim[1]][dim[2]];
            scores[1] = new int[dim[1]][dim[2]];
		
			
	    	System.out.println(node.next());
	    	
    	});
	    
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

//JavaRDD<String> sequencePairs = inputRDD.
//mapPartitionsWithIndex((index, value) -> {
//    if(index==0 && value.hasNext()){
//        value.next();
//        return value;
//    }else
//        return value;
//}, false);