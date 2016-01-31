package com.smx.spark.bio;

import java.io.BufferedWriter;
import java.io.File;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import com.smx.spark.bio.part.DNAPartitioner;
import com.smx.spark.bio.part.SDNASequence;
import com.smx.spark.bio.part.SPairwiseSequenceScorer;

import org.biojava.nbio.core.sequence.AccessionID;
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.compound.NucleotideCompound;
import org.biojava.nbio.alignment.FractionalIdentityScorer;
import org.biojava.nbio.alignment.GuideTree;
import org.biojava.nbio.alignment.NeedlemanWunsch;
import org.biojava.nbio.alignment.SimpleGapPenalty;
import org.biojava.nbio.alignment.SubstitutionMatrixHelper;
import org.biojava.nbio.alignment.template.GapPenalty;
import org.biojava.nbio.alignment.template.PairwiseSequenceScorer;
import org.biojava.nbio.alignment.template.Profile;
import org.biojava.nbio.alignment.template.SubstitutionMatrix;
import org.forester.phylogeny.data.Accession;
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
import scala.tools.nsc.settings.AdvancedScalaSettings.X;

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
			sequenceRDD = transformToPartitionSDNASequence(inputRDD, true); // <-- local = true 

		
		//writeSDNASequenceToHdfs(sequenceRDD);
		
		// filter removes duplicates, get approx half of cartesian table, i.e., 10 of 5*5
		JavaPairRDD<Tuple2<Integer, SDNASequence>, Tuple2<Integer, SDNASequence>> pairs = 
				sequenceRDD.cartesian(sequenceRDD).filter(x -> { return x._1()._1() > x._2()._1();});
		
		// stage 1: pairwise similarity calculation
		//TODO: whats the difference between foreach and foreachPartition?
//		pairs.foreachPartition(joinedSeq -> {
//			
//			while (joinedSeq.hasNext()) {
//				
//				Tuple2<Tuple2<Integer, SDNASequence>, Tuple2<Integer, SDNASequence>> sequencePair = joinedSeq.next();
//				
//				Tuple2<Integer, SDNASequence> tupleQuery = sequencePair._1();
//				Tuple2<Integer, SDNASequence> tupleTarget = sequencePair._2();
//				
//				
//						GapPenalty gapPenalty = new SimpleGapPenalty();
//						SubstitutionMatrix<NucleotideCompound> subMatrix = SubstitutionMatrixHelper.getNuc4_4();
//						
////						PairwiseSequenceScorer<DNASequence, NucleotideCompound> pairwiseScorer = 
////								new FractionalIdentityScorer<DNASequence, NucleotideCompound>(new 
////								NeedlemanWunsch<DNASequence, NucleotideCompound>(
////										tupleQuery._2().getDNASequence(), 
////										tupleTarget._2().getDNASequence(), 
////										gapPenalty, 
////										subMatrix));
////						
////						double score = pairwiseScorer.getScore();
//						
//						
//						NeedlemanWunsch<DNASequence, NucleotideCompound> nw = 
//								new NeedlemanWunsch<DNASequence, NucleotideCompound>(
//								tupleQuery._2().getDNASequence(), 
//								tupleTarget._2().getDNASequence(), 
//								gapPenalty, 
//								subMatrix);
//						
//						nw.getScore();
//
//						Profile<DNASequence, NucleotideCompound> profile = nw.getProfile();
//						
//						
//						logger.info("pairwise alignment (" + tupleQuery._1() + ", "  + tupleTarget._1() + ")");
//			}
//		});
	
		JavaPairRDD<Tuple2<Integer, Integer>, SPairwiseSequenceScorer> scorersRDD = pairs.mapToPair(pair -> {
			
			Tuple2<Integer, SDNASequence> tupleQuery = pair._1();
			Tuple2<Integer, SDNASequence> tupleTarget = pair._2();
			
			logger.info("pairwise alignment (" + tupleQuery._1() + ", "  + tupleTarget._1() + ")");

			GapPenalty gapPenalty = new SimpleGapPenalty();
			SubstitutionMatrix<NucleotideCompound> subMatrix = SubstitutionMatrixHelper.getNuc4_4();
			
			NeedlemanWunsch<DNASequence, NucleotideCompound> needlemanWunsch = 
					new NeedlemanWunsch<DNASequence, NucleotideCompound>(
					tupleQuery._2().getDNASequence(), 
					tupleTarget._2().getDNASequence(), 
					gapPenalty, 
					subMatrix);
			
			Tuple2<Tuple2<Integer, Integer>, SPairwiseSequenceScorer> 
			scorerSequence = new Tuple2<Tuple2<Integer, Integer>, SPairwiseSequenceScorer>
				(new Tuple2<Integer, Integer>(tupleQuery._1() ,tupleTarget._1()), new SPairwiseSequenceScorer(needlemanWunsch));

			return scorerSequence;
		});
		
		
		// stage 2: hierarchical clustering into a guide tree
		
		//       a:
		List<Tuple2<Tuple2<Integer, Integer>, SPairwiseSequenceScorer>> distances = scorersRDD.mapToPair(scorerSequence -> {
			return new Tuple2<Tuple2<Integer, Integer>, SPairwiseSequenceScorer>(new Tuple2<Integer, Integer>
			(scorerSequence._1()._1(), scorerSequence._1()._2()), scorerSequence._2());
		}).collect();
		
		// get all sequence input strings as list of string
		 List<String> accessionIds = sequenceRDD.map(sdnaSequence -> {
			 return sdnaSequence._2().getDNASequence().getAccession().getID();
		 }).collect();
		 
		// Need List of DNASequence that have all but actual sequence data
		List<DNASequence> sequences = accessionIds.stream().map( id -> {
			DNASequence dnaSequence = new DNASequence();
			dnaSequence.setAccession(new AccessionID(id));
			return dnaSequence;
		}).collect(Collectors.toList());
		
		List<SPairwiseSequenceScorer> scorers = distances.stream().map(dist -> {
			return dist._2();
		}).collect(Collectors.toList());
		
		
		@SuppressWarnings("rawtypes")
		GuideTree<DNASequence, NucleotideCompound> tree = new GuideTree(sequences, scorers);
		
		
		//       b:
		distances.forEach(distance -> {
			logger.info("(" + distance._1()._1() + ", " + distance._1()._2() 
					+ ") distance --> " + distance._2());
		});
		
		// stage 3: progressive alignment
		
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
		    	br.write(partDNASeq._2().getDNASequence().getSequenceAsString());
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
			        sdnaSequence.setSDNASequence(object.getObjectContent());
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
			        sdnaSequence.setSDNASequence(FileUtils.openInputStream(file));
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
									sdnaSequenceQuery.getDNASequence(), 
									sdnaSequenceTarget.getDNASequence(), 
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