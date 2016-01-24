package com.smx.spark.bio.part;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.io.FastaReaderHelper;
import org.biojava.nbio.core.sequence.io.FastaWriterHelper;

/** Proxy class for DNASequence implements Serializable
 * 
 * @author kevin
 *
 */
public class SDNASequence implements Serializable {

	private transient DNASequence dnaSequence;
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 2814589781198243348L;
	
	public SDNASequence() {
		//this.dnaSequence = null;
	}

	/**
	 * 
	 * @param stream
	 * @throws IOException
	 */
	private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {
		
		try {
			   
			if ( this.dnaSequence == null ) {
				return;
			}
			   
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
		
		if ( this.dnaSequence == null ) {
			return;
		}
		
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
