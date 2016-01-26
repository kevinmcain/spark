package com.smx.spark.bio.part;

import org.apache.spark.Partitioner;

public class DNAPartitioner extends Partitioner {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Integer partitions;
	Integer elements;
	
	public DNAPartitioner(Integer partitions, Integer elements) {
		this.partitions = partitions;
		this.elements = elements;
	}
	
	@Override
	public int getPartition(Object arg0) {
		Integer k = (Integer)arg0;
		return k * partitions / elements;
	}

	@Override
	public int numPartitions() {
		return partitions;
	}
	
}