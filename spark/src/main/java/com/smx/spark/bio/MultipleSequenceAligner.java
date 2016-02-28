package com.smx.spark.bio;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import com.smx.spark.bio.part.DNAPartitioner;
import com.smx.spark.bio.part.SDNASequence;
import com.smx.spark.bio.part.SPairwiseSequenceScorer;
import com.smx.spark.bio.part.ScorerPartitioner;

import org.biojava.nbio.core.sequence.AccessionID;
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.MultipleSequenceAlignment;
import org.biojava.nbio.core.sequence.compound.NucleotideCompound;
import org.biojava.nbio.core.sequence.template.Sequence;
import org.biojava.nbio.core.util.ConcurrencyTools;
import org.biojava.nbio.phylo.TreeConstructionAlgorithm;
import org.biojava.nbio.phylo.TreeConstructor;
import org.biojava.nbio.phylo.TreeType;
import org.biojava.nbio.alignment.FractionalIdentityScorer;
import org.biojava.nbio.alignment.GuideTree;
import org.biojava.nbio.alignment.NeedlemanWunsch;
import org.biojava.nbio.alignment.SimpleGapPenalty;
import org.biojava.nbio.alignment.SimpleProfileProfileAligner;
import org.biojava.nbio.alignment.SubstitutionMatrixHelper;
import org.biojava.nbio.alignment.template.AlignedSequence;
import org.biojava.nbio.alignment.template.CallableProfileProfileAligner;
import org.biojava.nbio.alignment.template.GapPenalty;
import org.biojava.nbio.alignment.template.GuideTreeNode;
import org.biojava.nbio.alignment.template.PairwiseSequenceScorer;
import org.biojava.nbio.alignment.template.Profile;
import org.biojava.nbio.alignment.template.ProfilePair;
import org.biojava.nbio.alignment.template.ProfileProfileAligner;
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
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
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
			logger.info("Starting Multiple Sequence Alignment at: " + InetAddress.getLocalHost().getHostName());	
		} catch(Exception e) { 
			logger.info(e.getMessage());	
		}

		// for deployment		
		JavaSparkContext sc = new JavaSparkContext(new SparkConf()
        	.setAppName("Bio Application")
        	//.set("spark.core.connection.ack.wait.timeout","600")
        	//.set("spark.driver.maxResultSize", "12g")
        	//.set("spark.driver.memory", "12g")
        	//.set("spark.executor.memory", "12g")
        	);
		
//		// for development
//		JavaSparkContext sc = new JavaSparkContext(new SparkConf()
//    		.setAppName("Bio Application")
//        	.setMaster("local[1]"));
		
		JavaRDD<String> inputRDD = sc.textFile(args[0]);
		
		Integer count = new Integer((int)inputRDD.count());
		
		JavaPairRDD<Integer, SDNASequence> 
			sequenceRDD = transformToPartitionSDNASequence(inputRDD); // <-- local = true 
		
		logger.info("sequenceRDD built");
		
		//writeSDNASequenceToHdfs(sequenceRDD);

		// cartesian renders the table above and filter removes those x,x pairs for a distinct set 
		JavaPairRDD<Tuple2<Integer, SDNASequence>, Tuple2<Integer, SDNASequence>> pairs = 
				sequenceRDD.cartesian(sequenceRDD).filter(x -> { return x._1()._1() < x._2()._1();});
		
		logger.info("pairwise similarity calculation - lazy transformation");
	
		// stage 1: pairwise similarity calculation - lazy transformation
		JavaPairRDD<Tuple2<Integer, Integer>, SPairwiseSequenceScorer> 
			scorersRDD = pairs.mapToPair(pair -> {
			
				Tuple2<Integer, SDNASequence> tupleQuery = pair._1();
				Tuple2<Integer, SDNASequence> tupleTarget = pair._2();
				
				logger.info("pairwise alignment (" + tupleQuery._1() + ", "  + tupleTarget._1() + ")");
	
				GapPenalty gapPenalty = new SimpleGapPenalty();
				SubstitutionMatrix<NucleotideCompound> subMatrix = SubstitutionMatrixHelper.getNuc4_4();
				
				// debug
//				DNASequence query = tupleQuery._2().getDNASequence();
//				DNASequence target = tupleQuery._2().getDNASequence();
//				
//				Tuple2<Tuple2<Integer, Integer>, SPairwiseSequenceScorer> 
//				scorerSequence = new Tuple2<Tuple2<Integer, Integer>, SPairwiseSequenceScorer>
//					(new Tuple2<Integer, Integer>(tupleQuery._1() ,tupleTarget._1()), new SPairwiseSequenceScorer());
				
				FractionalIdentityScorer<DNASequence, NucleotideCompound> fractionalIdentityScorer = 
						new FractionalIdentityScorer<DNASequence, NucleotideCompound>( 
						new NeedlemanWunsch<DNASequence, NucleotideCompound>(
						tupleQuery._2().getDNASequence(), 
						tupleTarget._2().getDNASequence(), 
						gapPenalty, 
						subMatrix));
				
				Tuple2<Tuple2<Integer, Integer>, SPairwiseSequenceScorer> 
				scorerSequence = new Tuple2<Tuple2<Integer, Integer>, SPairwiseSequenceScorer>
					(new Tuple2<Integer, Integer>(tupleQuery._1() ,tupleTarget._1()), new SPairwiseSequenceScorer(fractionalIdentityScorer));
	
				return scorerSequence;
		}).partitionBy(new ScorerPartitioner(count));
		
		
//		scorersRDD.foreachPartition( part -> {
//			while (part.hasNext()) {
//				Tuple2<Tuple2<Integer, Integer>, SPairwiseSequenceScorer> scorerSequence = part.next();
//				logger.info(scorerSequence._1()._1() +","+ scorerSequence._1()._2());
//				logger.info("it one");
//			}
//			logger.info("part one");
//		});
		
		// rdd.toLocalIterator lets you read one rdd partition at a time
		//scorersRDD.coalesce(10);
		
		logger.info("pairwise similarity calculation - call to action with collect");
		// stage 1: pairwise similarity calculation - call to action with collect
		List<SPairwiseSequenceScorer> scorers = scorersRDD.map(scorerSequence -> {
			return scorerSequence._2();
		}).collect();
		
		logger.info("collected scorers");
		
		List<DNASequence> dnaSequenceList = sequenceRDD.map(sequence -> {
			return sequence._2();
		}).collect().stream().map(seq -> {
			return seq.getDNASequence();
		}).collect(Collectors.toList());
		
		logger.info("collected sequences");
		
		logger.info("building guide tree");
		@SuppressWarnings("rawtypes")
		GuideTree<DNASequence, NucleotideCompound> tree 
			= new GuideTree(dnaSequenceList, scorers);
		
        // find inner nodes in post-order traversal of tree (each leaf node has a single sequence profile)
        List<GuideTreeNode<DNASequence, NucleotideCompound>> innerNodes 
        	= new ArrayList<GuideTreeNode<DNASequence, NucleotideCompound>>();
        
        for (GuideTreeNode<DNASequence, NucleotideCompound> n : tree) {
            if (n.getProfile() == null) {
                innerNodes.add(n);
            }
        }

		GapPenalty gapPenalty = new SimpleGapPenalty();
		SubstitutionMatrix<NucleotideCompound> subMatrix = SubstitutionMatrixHelper.getNuc4_4();
        
        // submit alignment tasks to the shared thread pool
        int i = 1, all = innerNodes.size();
        for (GuideTreeNode<DNASequence, NucleotideCompound> n : innerNodes) {
            
        	Profile<DNASequence, NucleotideCompound> p1 = n.getChild1().getProfile();
            Profile<DNASequence, NucleotideCompound> p2 = n.getChild2().getProfile();
    		
            Future<ProfilePair<DNASequence, NucleotideCompound>> pf1 = n.getChild1().getProfileFuture();
            Future<ProfilePair<DNASequence, NucleotideCompound>> pf2 = n.getChild2().getProfileFuture();
            
            ProfileProfileAligner<DNASequence, NucleotideCompound> aligner =
                    (p1 != null) ? ((p2 != null) ? new SimpleProfileProfileAligner<DNASequence, NucleotideCompound>(p1, p2, gapPenalty, subMatrix) :
                    	new SimpleProfileProfileAligner<DNASequence, NucleotideCompound>(p1, pf2, gapPenalty, subMatrix)) :
                    ((p2 != null) ? new SimpleProfileProfileAligner<DNASequence, NucleotideCompound>(pf1, p2, gapPenalty, subMatrix) :
                    	new SimpleProfileProfileAligner<DNASequence, NucleotideCompound>(pf1, pf2, gapPenalty, subMatrix));
            n.setProfileFuture(ConcurrencyTools.submit(new CallableProfileProfileAligner<DNASequence, NucleotideCompound>(aligner), String.format(
                    "Aligning pair %d of %d", i++, all)));
        }
        
        // retrieve the alignment results
        for (GuideTreeNode<DNASequence, NucleotideCompound> n : innerNodes) {
            // TODO when added to ConcurrencyTools, log completions and exceptions instead of printing stack traces
            try {
                n.setProfile(n.getProfileFuture().get());
            } catch (InterruptedException e) {
                logger.error("Interrupted Exception: ", e);
            } catch (ExecutionException e) {
                logger.error("Execution Exception: ", e);
            }
        }
		
        // stage 3: progressive alignment - finished
        logger.info("progressive alignment - finished");
        Profile<DNASequence, NucleotideCompound> msa = tree.getRoot().getProfile(); 
        
        buildTree(msa);
	}
	
	/**
	 * 
	 * @param msa
	 */
	private static void buildTree(Profile<DNASequence, NucleotideCompound> msa){
		MultipleSequenceAlignment<DNASequence, NucleotideCompound> 
		multipleSequenceAlignment= new MultipleSequenceAlignment <DNASequence, NucleotideCompound>();
	
		List<AlignedSequence<DNASequence,NucleotideCompound>> 
			alignedSequenceList = msa.getAlignedSequences();
		
		Sequence<NucleotideCompound> seq;
		DNASequence dnaSeq;
	       
		try {
			   
			for (int i = 0; i < alignedSequenceList.size(); i++) {     
				seq = alignedSequenceList.get(i);
				dnaSeq=new DNASequence(seq.getSequenceAsString(),seq.getCompoundSet());
				dnaSeq.setAccession(seq.getAccession());
				multipleSequenceAlignment.addAlignedSequence(dnaSeq);
			}   
		} 
		catch(Exception e) {
			System.out.printf(e.getMessage());
		}
	
		logger.info("tree constructor phase");
		
		TreeConstructor<DNASequence, NucleotideCompound> 
			treeConstructor = new TreeConstructor<DNASequence, NucleotideCompound>(
					multipleSequenceAlignment, 
					TreeType.NJ, 
					TreeConstructionAlgorithm.PID, 
					new ProgessListenerStub());
		try	{

			treeConstructor.process();   
			String newick = treeConstructor.getNewickString(true, true);
			
			logger.info("write newick output");
			//writeToFile("treeOutput/newick", newick);
			writeFileToS3Bucket("treeOutput/newick", newick);
		}
		catch (Exception e) {
			logger.info(e.getMessage());
		}
		
		ConcurrencyTools.shutdown();
	}
	
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
			        object.close();
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
	
	/** Writes the specified content to the specified filePath
	 * 
	 * @param filePath
	 * @param content
	 */
	private static void writeToFile(String filePath, String content) {
		try {
	    	Path path = new Path(filePath);
	    	Configuration configuration = new Configuration();
	    	FileSystem hdfs = path.getFileSystem(configuration);
	    	if ( hdfs.exists( path )) { hdfs.delete( path, true ); } 
	    	OutputStream os = hdfs.create(path, true);
	    	BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
	    	br.write(content);
	    	br.close();
	    	hdfs.close();
		} catch (Exception e) {
			logger.info(e.getMessage());
		}
	}
	
	/** Writes the specified content to the specified bucket
	 * 
	 * @param keyName
	 * @param content
	 */
	private static void writeFileToS3Bucket(String keyName, String content) {
		// automatically configure credentials from ec2 instance
		AmazonS3 s3Client = new AmazonS3Client(new InstanceProfileCredentialsProvider());
		ByteArrayInputStream input = new ByteArrayInputStream(content.getBytes());
		s3Client.putObject(new PutObjectRequest( S3_BUCKET, keyName, input, new ObjectMetadata()));
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