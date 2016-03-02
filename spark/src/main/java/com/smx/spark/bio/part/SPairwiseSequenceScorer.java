package com.smx.spark.bio.part;

import java.io.IOException;
import java.io.Serializable;

import org.biojava.nbio.alignment.FractionalIdentityScorer;
import org.biojava.nbio.alignment.NeedlemanWunsch;
import org.biojava.nbio.alignment.template.PairwiseSequenceScorer;
import org.biojava.nbio.alignment.template.Profile;
import org.biojava.nbio.alignment.template.SequencePair;
import org.biojava.nbio.core.sequence.DNASequence;
import org.biojava.nbio.core.sequence.compound.NucleotideCompound;
import org.biojava.nbio.core.sequence.io.FastaWriterHelper;
import org.biojava.nbio.core.sequence.template.Sequence;

/** Serializable PairwiseSequenceScorer, 
 * 		contains only that which is needed for multiple sequence alignment
 * 
 * @author kevin
 *
 */
public class SPairwiseSequenceScorer 
	implements PairwiseSequenceScorer<DNASequence, NucleotideCompound>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3355314671342154292L;
	
	//private transient Profile<DNASequence, NucleotideCompound> profile;
	//private SDNASequence query, target;
	
	private double distance;
	private double maxScore;
	private double minScore;
	private double score;
	private double similarity;
	
	public SPairwiseSequenceScorer() { }
	
	public SPairwiseSequenceScorer(FractionalIdentityScorer<DNASequence, NucleotideCompound> fractionalIdentityScorer) {
		
		distance = fractionalIdentityScorer.getDistance();
		maxScore = fractionalIdentityScorer.getMaxScore();
		minScore = fractionalIdentityScorer.getMinScore();
		score = fractionalIdentityScorer.getScore();
		similarity = fractionalIdentityScorer.getSimilarity();
	}

	@Override
	public double getDistance() {
		return distance;
	}

	@Override
	public double getDistance(double scale) {
		return 0;
	}

	@Override
	public double getMaxScore() {
		return maxScore;
	}

	@Override
	public double getMinScore() {
		return minScore;
	}

	@Override
	public double getScore() {
		return score;
	}

	@Override
	public double getSimilarity() {
		return similarity;
	}

	@Override
	public double getSimilarity(double scale) {
		return 0;
	}

	@Override
	public DNASequence getQuery() {
		return null; //query.getDNASequence();
	}

	@Override
	public DNASequence getTarget() {
		return null; //target.getDNASequence();
	}
	
	/**
	 * 
	 * @param stream
	 * @throws IOException
	 */
	private void writeObject(java.io.ObjectOutputStream stream)
            throws IOException {
		
		try {
			stream.defaultWriteObject();
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

		stream.defaultReadObject();
    }
}
