package com.smx.spark.bio;

import java.io.BufferedWriter;
import java.io.File;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.util.Iterator;

import com.smx.spark.bio.part.DNAPartitioner;
import com.smx.spark.bio.part.SDNASequence;

import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.compound.NucleotideCompound;
import org.biojava.nbio.alignment.FractionalIdentityScorer;
import org.biojava.nbio.alignment.NeedlemanWunsch;
import org.biojava.nbio.alignment.SimpleGapPenalty;
import org.biojava.nbio.alignment.SubstitutionMatrixHelper;
import org.biojava.nbio.alignment.template.GapPenalty;
import org.biojava.nbio.alignment.template.PairwiseSequenceScorer;
import org.biojava.nbio.alignment.template.SubstitutionMatrix;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.SparkConf;

import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;








import scala.Tuple2;

public class MultipleSequenceAligner {
	
	private final static String S3_BUCKET = "smx.spark.bio.bucket";
	static Logger logger = Logger.getLogger(MultipleSequenceAligner.class.getName());
	
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
		
		Broadcast<Integer> count = sc.broadcast((int)(long)inputRDD.count());
		
		JavaPairRDD<Integer, SDNASequence> 
			sequenceRDD = transformToPartitionSDNASequence(inputRDD, true);
		
//		sequenceRDD.foreach(line -> { 
//			logger.info(line._1().getId() + ", " + line._1().getFileName() + ", " + line._2().getPartitionId());			
//		});
		
//		JavaPairRDD<Tuple2<Integer, SDNASequence>, Tuple2<Partition, SDNASequence>> 
//			alignedSequenceRDD = sequenceRDD.cartesian(sequenceRDD);

//		JavaPairRDD<Integer, Tuple2<SDNASequence, SDNASequence>>
//			joinedRDD = sequenceRDD.join(sequenceRDD.sortByKey(new PartitionComparator(), false));

		// When called on datasets of type (K, V) and (K, W), 
		// returns a dataset of (K, (Iterable<V>, Iterable<W>)) tuples.

				
//		joinedRDD.foreach(seqPair -> { 
//			logger.info(seqPair._1().getId() + " -> " + seqPair._2()._1().getPartitionId() 
//					+ " and " + seqPair._2()._2().getPartitionId() );
//		});
		
		JavaPairRDD<Integer, SDNASequence> groupRDD = sequenceRDD.mapToPair
				(javaPairRDD -> { return new Tuple2<Integer, SDNASequence>
				(((javaPairRDD._1()+1)%count.value()), javaPairRDD._2());
		});
		
		JavaPairRDD<Integer,Tuple2<Iterable<SDNASequence>, Iterable<SDNASequence>>>
			groupWithSeqRDD = sequenceRDD.groupWith(groupRDD);
		
		
		//TODO: whats the difference between foreach and foreachPartition?
		groupWithSeqRDD.foreachPartition(seqGroup -> {
		
			while (seqGroup.hasNext()) {
				
				Tuple2<Integer,Tuple2<Iterable<SDNASequence>, Iterable<SDNASequence>>> tuple2 = seqGroup.next();
				Integer partition = tuple2._1();
				
				Tuple2<Iterable<SDNASequence>, Iterable<SDNASequence>> sequencePair = tuple2._2();
				Iterator<SDNASequence> itquery = sequencePair._1().iterator();
				
				while (itquery.hasNext()) {
		
					SDNASequence sdnaSequenceQuery = itquery.next();
					
					Iterator<SDNASequence> ittarget = sequencePair._2().iterator();
					while (ittarget.hasNext()) {
						SDNASequence sdnaSequenceTarget = ittarget.next();
						

//						GapPenalty gapPenalty = new SimpleGapPenalty();
//						SubstitutionMatrix<NucleotideCompound> subMatrix = SubstitutionMatrixHelper.getNuc4_4();
//						
//						PairwiseSequenceScorer<DNASequence, NucleotideCompound> pairwiseScorer = 
//								new FractionalIdentityScorer<DNASequence, NucleotideCompound>(new 
//								NeedlemanWunsch<DNASequence, NucleotideCompound>(
//										sdnaSequenceQuery.getDnaSequence(), 
//										sdnaSequenceTarget.getDnaSequence(), 
//										gapPenalty, 
//										subMatrix));
//						
//						double score = pairwiseScorer.getScore();
						
						logger.info("pairwise alignment " + partition.toString() + " -> ( " 
								+ sdnaSequenceQuery.getPartitionId() + ", " 
								+ sdnaSequenceTarget.getPartitionId() + ")");
					}				
				}
			}
		});
		
		
		
//		JavaPairRDD<Integer, Iterable<SDNASequence>>
//			groupRDD = sequenceRDD.groupByKey();
//		
//		groupRDD.foreach(seq -> {
//			
//			Iterator<SDNASequence> iter = seq._2().iterator();
//			
//			while(iter.hasNext()) {
//				SDNASequence sdnaSequence = iter.next();
//				logger.info("partition " +seq._1() + "," + sdnaSequence.getPartitionId());
//			}
//		});
		
		
		//writeSDNASequenceToHdfs(sequenceRDD);
	}
	

	
//	"Any fool can write code that a computer can understand. Good programmers write code that humans can understand." --- Martin Fowler 
//	Please correct my English.
	
	/** Writes all dna sequences to hdfs 
	 * 
	 * @param sequenceRDD
	 */
	private static void writeSDNASequenceToHdfs
		(JavaPairRDD<Integer, SDNASequence> sequenceRDD) {
		
		logger.info("writing data to hdfs");
		
		sequenceRDD.foreachPartition(record -> {
			
			if (record.hasNext()) {
			
				Tuple2<Integer, SDNASequence> partDNASeq = record.next();

		    	Path path = new Path(partDNASeq._2().getFileName());
		    	Configuration configuration = new Configuration();
		    	FileSystem hdfs = path.getFileSystem(configuration);
		    	if ( hdfs.exists( path )) { hdfs.delete( path, true ); } 
		    	OutputStream os = hdfs.create(path, true);
		    	BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
		    	br.write(partDNASeq._2().getSequence());
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
	private static JavaPairRDD<Integer, SDNASequence> 
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
			        sdnaSequence.setSequence(object.getObjectContent());
			        sdnaSequence.setPartitionId(id);
			        sdnaSequence.setFileName(keyName);

					return new Tuple2<Integer, SDNASequence>(id, sdnaSequence);
				})
				.partitionBy(new DNAPartitioner(count, count));
	}
	
	/** Partition the input according to the id and keyName (S3 fileName) "local fileSystem"
 	 * 
	 * @param inputRDD
	 * @param local
	 * @return
	 */
	private static JavaPairRDD<Integer, SDNASequence> 
		transformToPartitionSDNASequence(JavaRDD<String> inputRDD, Boolean local) {
		
		if (!local) return transformToPartitionSDNASequence(inputRDD);  
		
		Integer count = new Integer((int)inputRDD.count());
		
		return inputRDD.mapToPair((line) ->  {
					String[] idAndKeyName = line.split(",");
					Integer id = Integer.parseInt(idAndKeyName[0]);
					String keyName = idAndKeyName[1]; 
					
					SDNASequence sdnaSequence = new SDNASequence();
					File file = new File(keyName);					
			        sdnaSequence.setSequence(FileUtils.openInputStream(file));
			        sdnaSequence.setPartitionId(id);
			        sdnaSequence.setFileName(keyName);
			        
					return new Tuple2<Integer, SDNASequence>(id, sdnaSequence);
				})
				.partitionBy(new DNAPartitioner(count, count));
	}
	
	/**
	 * 
	 * @param groupWithSeqRDD
	 */
	private void runAlignment(JavaPairRDD<Integer,
			Tuple2<Iterable<SDNASequence>, Iterable<SDNASequence>>> groupWithSeqRDD) {
		groupWithSeqRDD.foreach(seqGroup -> {
			
			Iterator<SDNASequence> itquery = seqGroup._2()._1().iterator();
			
			while (itquery.hasNext()) {
	
				SDNASequence sdnaSequenceQuery = itquery.next();
				
				Iterator<SDNASequence> ittarget = seqGroup._2()._2().iterator();
				while (ittarget.hasNext()) {
					SDNASequence sdnaSequenceTarget = ittarget.next();
					

					GapPenalty gapPenalty = new SimpleGapPenalty();
					SubstitutionMatrix<NucleotideCompound> subMatrix = SubstitutionMatrixHelper.getNuc4_4();
					
					PairwiseSequenceScorer<DNASequence, NucleotideCompound> pairwiseScorer = 
							new FractionalIdentityScorer<DNASequence, NucleotideCompound>(new 
							NeedlemanWunsch<DNASequence, NucleotideCompound>(
									sdnaSequenceQuery.getDnaSequence(), 
									sdnaSequenceTarget.getDnaSequence(), 
									gapPenalty, 
									subMatrix));
					
					double score = pairwiseScorer.getScore();
					
//					logger.info("pairwise alignment " + seqGroup._1() + " -> ( " 
//							+ sdnaSequenceQuery.getPartitionId() + ", " 
//							+ sdnaSequenceTarget.getPartitionId() + ")");
				}				
			}
		});
	}
}