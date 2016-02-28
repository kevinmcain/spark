package com.smx.spark.bio.part;

import org.apache.spark.Partitioner;

import scala.Tuple2;

public class ScorerPartitioner extends Partitioner {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Integer sequences;
	Integer partitions;
	
	public ScorerPartitioner(Integer sequences) {
		this.sequences = sequences;
		Double count = new Double((int)sequences);
		count--;
		count = .5*(count*(count+1));
		this.partitions = count.intValue();
	}
	
	@Override
	public int getPartition(Object arg0) {
		Tuple2<Integer, Integer> pair = (Tuple2<Integer, Integer>)arg0;
		Integer k=0;
		for (Integer i=0; i < sequences; i++) {
			for (Integer j=i+1; j < sequences; j++) {
				if (pair._1().equals(i) && pair._2().equals(j)) {
					return k;
				}
				k++;
			}
		}
		return k;
	}

	@Override
	public int numPartitions() {
		return partitions;
	}
	
}