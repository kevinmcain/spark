package com.smx.spark.bio.part;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.io.FastaReaderHelper;
import org.biojava.nbio.core.sequence.io.FastaWriterHelper;

/** Proxy class for DNASequence implements Serializable
 * 
 * @author kevin
 *
 */
public class SDNASequence implements Serializable {

	private DNASequence dnaSequence;
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 2814589781198243348L;

	/**
	 * 
	 * @param stream
	 * @throws IOException
	 */
	private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {
		
	   try {
		   FastaWriterHelper.writeSequence(stream, this.dnaSequence);
	   } catch (Exception e) {
		   throw new IOException(e.getMessage());
	   }

		//http://stackoverflow.com/questions/12963445/serialization-readobject-writeobject-overides		
		//        stream.writeObject(name);
		//        stream.writeInt(id);
		//        stream.writeObject(DOB);
    }
	
	/**
	 * 
	 * @param stream
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private void readObject(java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException {
		
    	Map<String, DNASequence> linkedHashMap = 
    			FastaReaderHelper.readFastaDNASequence(stream);

		List<DNASequence> list = 
				new ArrayList<DNASequence>(linkedHashMap.values());
		
		this.dnaSequence = list.get(0);
    	
		//http://stackoverflow.com/questions/12963445/serialization-readobject-writeobject-overides    	
		//        name = (String) stream.readObject();
		//        id = stream.readInt();
		//        DOB = (String) stream.readObject();
    }
}
