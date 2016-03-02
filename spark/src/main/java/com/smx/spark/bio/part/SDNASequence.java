package com.smx.spark.bio.part;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.io.FastaReaderHelper;
import org.biojava.nbio.core.sequence.io.FastaWriterHelper;

/** Proxy class for DNASequence implements Serializable.
 * 
 * @author kevin
 * 
 *  http://www.javapractices.com/topic/TopicAction.do?Id=45
 *	http://stackoverflow.com/questions/12963445/serialization-readobject-writeobject-overides
 *
 */
public class SDNASequence implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2814589781198243348L;
	
	private transient DNASequence dnaSequence;

	public SDNASequence() {
		//this.dnaSequence = null;
	}
	
	public DNASequence getDNASequence() {
		return dnaSequence;
	}

	public void setDNASequence(DNASequence dnaSequence) {
		this.dnaSequence = dnaSequence;
	}

	public void setSDNASequence(java.io.InputStream input) throws IOException {

    	Map<String, DNASequence> linkedHashMap = 
    			FastaReaderHelper.readFastaDNASequence(input);

		List<DNASequence> list = 
				new ArrayList<DNASequence>(linkedHashMap.values());
		
		// Attach writing host for diagnostics -> InetAddress.getLocalHost().getHostName();
		this.setDNASequence(list.get(0));
	}

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
    }
	
	/** https://docs.oracle.com/javase/7/docs/api/java/io/ObjectInputStream.html
	 * 
	 * @param stream
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private void readObject(java.io.ObjectInputStream stream)
            throws IOException, ClassNotFoundException {
		
		try {
			
			// must copy input stream before passing into FastaReaderHelper.readFastaDNASequence
			// this prevents the issue of 'WARN TaskSetManager: Lost task ...
			// io.netty.util.IllegalReferenceCountException: refCnt: 0
			
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			byte[] buffer = new byte[1024];

			int len;
			while ((len = stream.read(buffer)) > -1 ) {
			    baos.write(buffer, 0, len);
			}
			baos.flush();

			// Open new InputStreams using the recorded bytes
			InputStream input = new ByteArrayInputStream(baos.toByteArray());

			this.setSDNASequence(input);
			
		} catch (Exception e) {
			throw new IOException(e.getMessage());
		}
    }
}
