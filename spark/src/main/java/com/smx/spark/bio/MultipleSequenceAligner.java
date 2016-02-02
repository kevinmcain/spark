package com.smx.spark.bio;

import java.io.BufferedWriter;
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

import org.biojava.nbio.core.sequence.AccessionID;
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.MultipleSequenceAlignment;
import org.biojava.nbio.core.sequence.compound.NucleotideCompound;
import org.biojava.nbio.core.sequence.template.Sequence;
import org.biojava.nbio.core.util.ConcurrencyTools;
import org.biojava.nbio.phylo.TreeConstructionAlgorithm;
import org.biojava.nbio.phylo.TreeConstructor;
import org.biojava.nbio.phylo.TreeType;
import org.biojava.nbio.alignment.Alignments.ProfileProfileAlignerType;
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
			sequenceRDD = transformToPartitionSDNASequence(inputRDD, true); // <-- local = true 

		
		//writeSDNASequenceToHdfs(sequenceRDD);
		
		
		//		x,x 0,1 0,2 0,3 0,4
		//		x,x x,x 1,2 1,3 1,4
		//		x,x x,x x,x 2,3 2,4
		//		x,x x,x x,x x,x 3,4
		//		x,x x,x x,x x,x x,x

		// cartesian renders the table above and filter removes those x,x pairs for a distinct set 
		JavaPairRDD<Tuple2<Integer, SDNASequence>, Tuple2<Integer, SDNASequence>> pairs = 
				sequenceRDD.cartesian(sequenceRDD).filter(x -> { return x._1()._1() < x._2()._1();});
	
		JavaPairRDD<Tuple2<Integer, Integer>, SPairwiseSequenceScorer> 
			scorersRDD = pairs.mapToPair(pair -> {
			
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
			DNASequence dnaSequence = null;
			try {
				dnaSequence = new DNASequence("ATCG");
				dnaSequence.setAccession(new AccessionID(id));
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return dnaSequence;
		}).collect(Collectors.toList());
		
		List<SPairwiseSequenceScorer> scorers = distances.stream().map(dist -> {
			return dist._2();
		}).collect(Collectors.toList());
		
		
		@SuppressWarnings("rawtypes")
		GuideTree<DNASequence, NucleotideCompound> tree = new GuideTree(sequences, scorers);
		
//		writeToFile("tree", tree.toString());
		
//		distances.forEach(distance -> {  
//			logger.info("(" + distance._1()._1() + ", " + distance._1()._2() 
//					+ ") distance --> " + distance._2());
//		});
		
		
		
		// TODO: need to distribute alignment tasks below. refactor from use of executer to spark cluster
		//       its probably worth collecting the sequences first in order to determine if the output is
		//       in line with the output from the phylogen project solution.
		// stage 3: progressive alignment
		
        // find inner nodes in post-order traversal of tree (each leaf node has a single sequence profile)
        List<GuideTreeNode<DNASequence, NucleotideCompound>> innerNodes = new ArrayList<GuideTreeNode<DNASequence, NucleotideCompound>>();
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
        Profile<DNASequence, NucleotideCompound> msa = tree.getRoot().getProfile(); 
        buildTree(msa); 
        
	}
	
	/** Are we on the right track?
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
	
		TreeConstructor<DNASequence, NucleotideCompound> 
			treeConstructor = new TreeConstructor<DNASequence, NucleotideCompound>(
					multipleSequenceAlignment, 
					TreeType.NJ, 
					TreeConstructionAlgorithm.PID, 
					new ProgessListenerStub());
		try	{
			treeConstructor.process();   
			String newick = treeConstructor.getNewickString(true, true);
			logger.info(newick);
		}
		catch (Exception e) {
			logger.info(e.getMessage());
		}
		
		ConcurrencyTools.shutdown();

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