package com.smx.spark.bio.part;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.compound.NucleotideCompound;
import org.biojava.nbio.core.sequence.io.FastaReaderHelper;
import org.biojava.nbio.core.sequence.io.FastaWriterHelper;
import org.biojava.nbio.core.sequence.storage.ArrayListSequenceReader;
import org.biojava.nbio.core.sequence.template.SequenceReader;

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
	// DNASequence is an AbstractSequence which has a sequenceStorage member 
	// that is a SequenceReader<C> which is instantiated as ArrayListSequenceReader
	private transient ArrayListSequenceReader<NucleotideCompound> proxyLoader; //sequenceStorage;

	// This is a member of ArrayListSequenceReader
	private List<NucleotideCompound> parsedCompounds;
	private Integer partitionId;
	private String fileName = "";
	
	public SDNASequence() {
		//this.dnaSequence = null;
	}
	
	public DNASequence getDNASequence() {
		if (dnaSequence == null){
			proxyLoader = new ArrayListSequenceReader<NucleotideCompound>();
			proxyLoader.setContents(parsedCompounds);
			dnaSequence = new DNASequence(proxyLoader);
		}
			
		return dnaSequence;
	}

	private void setDNASequence(DNASequence dnaSequence) {
		this.dnaSequence = dnaSequence;
		this.proxyLoader = (ArrayListSequenceReader<NucleotideCompound>)dnaSequence.getProxySequenceReader();
		this.parsedCompounds = proxyLoader.getAsList(); 
	}

	public void setSDNASequence(java.io.InputStream stream) throws IOException {

    	Map<String, DNASequence> linkedHashMap = 
    			FastaReaderHelper.readFastaDNASequence(stream);

		List<DNASequence> list = 
				new ArrayList<DNASequence>(linkedHashMap.values());
		
		// Attach writing host for diagnostics -> InetAddress.getLocalHost().getHostName();
		this.setDNASequence(list.get(0));
	}
	
	public Integer getPartitionId() {
		return this.partitionId;
	}

	public void setPartitionId(Integer partitionId) {
		this.partitionId = partitionId;
	}
	
	public String getFileName() {
		return this.fileName;
	}
	
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	
	/**
	 * 
	 * @param stream
	 * @throws IOException
	 */
	private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {
		
		try {
			stream.writeObject(this.partitionId);
			stream.writeObject(this.fileName);
			//stream.writeObject(this.parsedCompounds.toString());
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

		this.partitionId = (Integer)stream.readObject();
		this.fileName = (String) stream.readObject();
		// the final part of the stream is a fasta file input stream
		this.setSDNASequence(stream);
    }
}
