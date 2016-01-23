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
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.io.FastaReaderHelper;
import org.biojava.nbio.core.sequence.io.FastaWriterHelper;

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

}
