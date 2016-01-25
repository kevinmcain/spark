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

import javax.xml.crypto.dsig.Transform;

import com.smx.spark.bio.part.Partition;
import com.smx.spark.bio.part.DNAPartitioner;
import com.smx.spark.bio.part.SDNASequence;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.*;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.io.FastaReaderHelper;
import org.biojava.nbio.core.sequence.io.FastaWriterHelper;

import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import scala.Tuple2;

public class NeedlmanWunsch {
	
	private final static String S3_BUCKET = "smx.spark.bio.bucket";
	static Logger logger = Logger.getLogger(NeedlmanWunsch.class.getName());
	
	// program arguments: src/main/resources/sequencePairs.txt
	public static void main(String[] args) {
		 
		//WriteDNASequence.wholeTextFilesBug();
		
		try {
			logger.info("starting NeedlmanWunsch at " + InetAddress.getLocalHost().getHostName());	
		} catch(Exception e) {
			logger.info(e.getMessage());	
		}
		
//		// for deployment		
//		JavaSparkContext sc = new JavaSparkContext(new SparkConf()
//        	.setAppName("Bio Application"));
		
		// for development
		JavaSparkContext sc = new JavaSparkContext(new SparkConf()
    		.setAppName("Bio Application")
        	.setMaster("local[1]"));
		
		JavaRDD<String> inputRDD = sc.textFile(args[0]);
		
		JavaPairRDD<Partition, SDNASequence> 
			sequenceRDD = transformToPartitionSDNASequence(inputRDD, true);

		sequenceRDD.foreach(line -> { 
			logger.info(line._1().getId() + "," + line._1().getFileName());			
		});
		
		//writeSDNASequenceToHdfs(sequenceRDD);
	}
	
	/** Writes all dna sequences to hdfs 
	 * 
	 * @param sequenceRDD
	 */
	private static void writeSDNASequenceToHdfs
		(JavaPairRDD<Partition, SDNASequence> sequenceRDD) {
		
		logger.info("writing data to hdfs");
		
		sequenceRDD.foreachPartition(record -> {
			
			if (record.hasNext()) {
			
				Tuple2<Partition, SDNASequence> partDNASeq = record.next();

		    	Path path = new Path(partDNASeq._1().getFileName());
		    	Configuration configuration = new Configuration();
		    	FileSystem hdfs = path.getFileSystem(configuration);
		    	if ( hdfs.exists( path )) { hdfs.delete( path, true ); } 
		    	OutputStream os = hdfs.create(path, true);
		    	BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
		    	br.write(partDNASeq._2().getDNASequence());
		    	br.write(InetAddress.getLocalHost().getHostName());
		    	br.close();
		    	hdfs.close();
			}
		});
	}
	
	/** Partition the input according to the id and keyName (S3 fileName)
	 * 
	 * @param inputRDD
	 * @return
	 */
	private static JavaPairRDD<Partition, SDNASequence> 
		transformToPartitionSDNASequence(JavaRDD<String> inputRDD) {
		
		logger.info("loading data from S3 bucket");
		
		Integer count = new Integer((int)inputRDD.count());
		
		return inputRDD.mapToPair((line) ->  {
					String[] idAndKeyName = line.split(",");
					Integer id = Integer.parseInt(idAndKeyName[0]);
					String keyName = idAndKeyName[1]; 
					
					SDNASequence sdnaSequence = new SDNASequence();
					
					// automatically configure credentials from ec2 instance
					AmazonS3 s3Client = new AmazonS3Client(new InstanceProfileCredentialsProvider());
					
					// get input from s3 bucket
			    	GetObjectRequest request = new GetObjectRequest(S3_BUCKET, keyName);
			    	
			        S3Object object = s3Client.getObject(request); 
			        sdnaSequence.setDNASequence(object.getObjectContent());

					return new Tuple2<Partition, SDNASequence>
						(new Partition(id, keyName), sdnaSequence);
				})
				.partitionBy(new DNAPartitioner(count, count));
	}
	
	/** Partition the input according to the id and keyName (S3 fileName) "local fileSystem"
 	 * 
	 * @param inputRDD
	 * @param local
	 * @return
	 */
	private static JavaPairRDD<Partition, SDNASequence> 
		transformToPartitionSDNASequence(JavaRDD<String> inputRDD, Boolean local) {
		
		if (!local) return transformToPartitionSDNASequence(inputRDD);  
		
		Integer count = new Integer((int)inputRDD.count());
		
		return inputRDD.mapToPair((line) ->  {
					String[] idAndKeyName = line.split(",");
					Integer id = Integer.parseInt(idAndKeyName[0]);
					String keyName = idAndKeyName[1]; 
					
					SDNASequence sdnaSequence = new SDNASequence();
					File file = new File(keyName);					
			        sdnaSequence.setDNASequence(FileUtils.openInputStream(file));

					return new Tuple2<Partition, SDNASequence>
						(new Partition(id, keyName), sdnaSequence);
				})
				.partitionBy(new DNAPartitioner(count, count));
	}
}