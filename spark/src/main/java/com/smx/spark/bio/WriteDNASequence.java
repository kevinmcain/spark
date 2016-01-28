package com.smx.spark.bio;

import java.io.BufferedWriter;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.io.FastaReaderHelper;
import org.biojava.nbio.core.sequence.io.FastaWriterHelper;

import scala.Tuple2;

import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class WriteDNASequence {
	
	static Logger logger = Logger.getLogger(MultipleSequenceAligner.class.getName());
	
	public WriteDNASequence() {
		writeDNASequence();
	}
	
	public  WriteDNASequence(String[] args) {
		writeDNASequenceFromClusterNodes(args);
	}
	
	/** There is a bug with sc wholeTextFiles function reading from S3 buckets
	 * http://stackoverflow.com/questions/31575367/loading-data-from-aws-s3-through-apache-spark
	 * https://issues.apache.org/jira/browse/SPARK-4414
	 * http://michaelryanbell.com/processing-whole-files-spark-s3.html
	 */
	public static void wholeTextFilesBug() {
		try {
			logger.info("starting NeedlmanWunsch at " + InetAddress.getLocalHost().getHostName());	
		} catch(Exception e) {
			logger.info(e.getMessage());	
		}
		
		// for deployment		
		JavaSparkContext sc = new JavaSparkContext(new SparkConf()
        	.setAppName("Bio Application"));
		
//		// for development
//		JavaSparkContext sc = new JavaSparkContext(new SparkConf()
//    		.setAppName("Bio Application")
//        	.setMaster("local[1]"));
		
		//JavaRDD<String> inputRDD = sc.textFile(args[0]); // partition to number of seq pairs
		//JavaRDD<String> inputRDD = sc.textFile("src/main/resources/sequencePairs.txt",3); // partition to number of seq pairs
		
		logger.info("starting wholeTextFiles");
		
		// C:/genomes/seq
		JavaPairRDD<String, String> rddAll = sc.wholeTextFiles
				("s3://smx.spark.bio.bucket//seq//*").partitionBy(new AutoPartitioner(3,3));

		logger.info("starting foreach");

		rddAll.foreach(line -> { 
			logger.info(line._1());
			logger.info(InetAddress.getLocalHost().getHostName());
		});

		rddAll.foreachPartition(record -> {
			
			if (record.hasNext()) {
			
				// InetAddress.getLocalHost().getHostName()
				Tuple2<String, String> dnaSeq = record.next();

				String[] parts = dnaSeq._1().split("/");
				String fileName = parts[parts.length-1];
				
		    	Path path = new Path("out/"+fileName);
		    	Configuration configuration = new Configuration();
		    	FileSystem hdfs = path.getFileSystem(configuration);
		    	if ( hdfs.exists( path )) { hdfs.delete( path, true ); } 
		    	OutputStream os = hdfs.create(path, true);
		    	BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
		    	br.write(dnaSeq._2());
		    	br.write(InetAddress.getLocalHost().getHostName());
		    	br.close();
		    	hdfs.close();
			}
		});
		
		logger.info("NeedlmanWunsch complete");
	}
	
	
	/** Have tested on master node in EMR cluster
	 *  to verify output.txt, run cmd from emr shell> hdfs dfs -cat output.txt
	 * 
	 */
	private void writeDNASequence() {
		try {
			
			// automatically configure credentials from master instance
			AmazonS3 s3Client = new AmazonS3Client(new InstanceProfileCredentialsProvider());
			// get input from s3 bucket
	    	GetObjectRequest request = new GetObjectRequest("smx.spark.bio.bucket","input/aone.fna");
	        S3Object object = s3Client.getObject(request);
	        InputStream in = object.getObjectContent();	
			
	    	Map<String, DNASequence> linkedHashMap = 
	    			FastaReaderHelper.readFastaDNASequence(in); //input

			List<DNASequence> list = 
					new ArrayList<DNASequence>(linkedHashMap.values());

	    	//File file = new File(list.get(0).getAccession().toString());
			File file = new File("test");
	    		FastaWriterHelper.writeSequence(file, list.get(0));
	    	
			logger.info("attempting to write to hdfs");


	    	Configuration configuration = new Configuration();
	    	logger.info("fs.default.name : - " +configuration.get("fs.defaultFS"));
	    	
	    	Path path = new Path("output.txt");
	    	FileSystem hdfs = path.getFileSystem(configuration);
	    	//if ( hdfs.exists( path )) { hdfs.delete( path, true ); } 
	    	OutputStream os = hdfs.create(path, true);
	    	BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
	    	br.write(list.get(0).toString());
	    	br.close();
	    	hdfs.close();
	    	
	    	logger.info("done attempting to write to hdfs");
		
		} catch(Exception e) {
			logger.info(e.getMessage());
		}
	}
		
	/** Have tested on Spark master node in EMR cluster. Cluster nodes
	 *  write output to hdfs. To see run cmd > hdfs dfs -ls input
	 * 
	 * @param args
	 */
	public static void writeDNASequenceFromClusterNodes(String[] args) {
		
		//s3://smx.spark.bio.bucket//input//sequencePairs.txt
		//0,input/aone.fna,src/main/resources/aone.fna
		//1,input/atwo.fna,src/main/resources/aone.fna
		//2,input/athree.fna,src/main/resources/atwo.fna

		logger.info("writeDNASequenceFromClusterNodes");
	
		//for deployment		
		JavaSparkContext sc = new JavaSparkContext(new SparkConf()
	    	.setAppName("Bio Application"));
	
		// //for local				
		//JavaSparkContext sc = new JavaSparkContext(new SparkConf()
		//	.setAppName("Bio Application")
		//	.setMaster("local[1]"));
		
		JavaRDD<String> inputRDD = sc.textFile(args[0]); // partition to number of seq pairs
		
		JavaPairRDD<Integer, String> rdd = inputRDD
				.mapToPair((s) ->  {
					Integer i = Integer.parseInt(s.substring(0, 1));
					
					return new Tuple2<Integer, String>(i, s.substring(2));
				})
				.partitionBy(new CustomPartitioner(3,3));
	
		
		rdd.foreach(line -> { 
			logger.info(line);	
		});
		
		rdd.foreachPartition(node -> { 
	
	        if (node.hasNext()){
	        	
	        	Tuple2<Integer, String> seqPair = node.next();
	        	String[] files = seqPair._2().split(",");
	        	String queryKey = files[0];
	        	//String targetFileName = files[1];
	        	try {
	
	
	    			// automatically configure credentials from master instance
	    			AmazonS3 s3Client = new AmazonS3Client(new InstanceProfileCredentialsProvider());
	    			// get input from s3 bucket
	    	    	GetObjectRequest request = new GetObjectRequest("smx.spark.bio.bucket",queryKey);
	    	        S3Object object = s3Client.getObject(request);
	    	        InputStream in = object.getObjectContent();	
	    			
	    	    	Map<String, DNASequence> linkedHashMap = 
	    	    			FastaReaderHelper.readFastaDNASequence(in); //input
	
	    			List<DNASequence> list = 
	    					new ArrayList<DNASequence>(linkedHashMap.values());
	
	    	    	//File file = new File(list.get(0).getAccession().toString());
	    			File file = new File("test");
	    	    		FastaWriterHelper.writeSequence(file, list.get(0));
	    	    	Configuration configuration = new Configuration();
	    	    	
	    	    	Path path = new Path(queryKey);
	    	    	FileSystem hdfs = path.getFileSystem(configuration);
	    	    	//if ( hdfs.exists( path )) { hdfs.delete( path, true ); } 
	    	    	OutputStream os = hdfs.create(path, true);
	    	    	BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
	    	    	br.write(list.get(0).toString());
	    	    	br.close();
	    	    	hdfs.close();
	        		
	        	} catch(Exception e) {
	        		//logger.info(e.getMessage());
	        	}
	        }
	        
		});
		
		logger.info("writeDNASequenceFromClusterNodes complete");;
	}

}

//
////JavaPairRDD<Integer, DNASequence> dna = sc.textFile("s3://smx.spark.bio.bucket//input//aone.fna,s3://smx.spark.bio.bucket//input//aone.fna,s3://smx.spark.bio.bucket//input//aone.fna")
//JavaPairRDD<Integer, DNASequence> dna = sc.textFile("C:/genomes/seq/1.fna,C:/genomes/seq/2.fna,C:/genomes/seq/3.fna")
//		.mapToPair((s) ->  {
//			Integer i = Integer.parseInt(s.substring(0, 1));
//			return new Tuple2<Integer, DNASequence>(i, new DNASequence(s.substring(2)));
//})
//.partitionBy(new CustomPartitioner(3,3));
//
//JavaPairRDD<Integer, DNASequence> dnaRDD = seqRDD
//		.mapToPair((seq) ->  {
//			return new Tuple2<Integer, DNASequence>(seq._1(), new DNASequence(seq._2()));
//		})
//		.partitionBy(new CustomPartitioner(3,3));
//
//dna.foreachPartition(record -> {
//	
//	if (record.hasNext()) {
//	
//		// InetAddress.getLocalHost().getHostName()
//		Tuple2<Integer, DNASequence> dnaSeq = record.next();
//		
//    	Path path = new Path("out/"+dnaSeq._2().getAccession());
//    	Configuration configuration = new Configuration();
//    	FileSystem hdfs = path.getFileSystem(configuration);
//    	if ( hdfs.exists( path )) { hdfs.delete( path, true ); } 
//    	OutputStream os = hdfs.create(path, true);
//    	BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
//    	br.write(dnaSeq._2().toString());
//    	br.write(InetAddress.getLocalHost().getHostName());
//    	br.close();
//    	hdfs.close();
//	}
//});
//
//logger.info("NeedlmanWunsch complete");

class AutoPartitioner extends Partitioner {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Integer partitions;
	Integer elements;
	Integer k = 0;
	
	public AutoPartitioner(Integer partitions, Integer elements) {
		this.partitions = partitions;
		this.elements = elements;
	}
	
	@Override
	public int getPartition(Object arg0) {
		Integer partition = k * partitions / elements;
		k++;
		return partition;
	}

	@Override
	public int numPartitions() {
		return partitions;
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


//spark-nndescent/src/main/java/info/debatty/spark/nndescent/ExampleStringCosineSimilarity.java

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

//java-graphs/src/main/java/info/debatty/java/graphs/Node.java
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



// ###

//sequenceRDD.foreach(line -> { 
//logger.info(line._1().getId() + ", " + line._1().getFileName() + ", " + line._2().getPartitionId());			
//});

//JavaPairRDD<Tuple2<Integer, SDNASequence>, Tuple2<Partition, SDNASequence>> 
//alignedSequenceRDD = sequenceRDD.cartesian(sequenceRDD);

//JavaPairRDD<Integer, Tuple2<SDNASequence, SDNASequence>>
//joinedRDD = sequenceRDD.join(sequenceRDD.sortByKey(new PartitionComparator(), false));

// When called on datasets of type (K, V) and (K, W), 
// returns a dataset of (K, (Iterable<V>, Iterable<W>)) tuples.

	
//joinedRDD.foreach(seqPair -> { 
//logger.info(seqPair._1().getId() + " -> " + seqPair._2()._1().getPartitionId() 
//		+ " and " + seqPair._2()._2().getPartitionId() );
//});


//JavaPairRDD<Integer, Iterable<SDNASequence>>
//groupRDD = sequenceRDD.groupByKey();
//
//groupRDD.foreach(seq -> {
//
//Iterator<SDNASequence> iter = seq._2().iterator();
//
//while(iter.hasNext()) {
//	SDNASequence sdnaSequence = iter.next();
//	logger.info("partition " +seq._1() + "," + sdnaSequence.getPartitionId());
//}
//});