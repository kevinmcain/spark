package com.smx.spark.bio;

import java.io.BufferedWriter;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
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
	
	static Logger logger = Logger.getLogger(NeedlmanWunsch.class.getName());
	
	public WriteDNASequence() {
		writeDNASequence();
	}
	
	public  WriteDNASequence(String[] args) {
		writeDNASequenceFromClusterNodes(args);
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
