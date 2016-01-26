package com.smx.spark.bio.part;

import java.io.Serializable;
import java.util.Comparator;

// TODO: Does this get passed serialiable?
//		Comparator<Partition> byPartitionId = 
// (Partition p1, Partition p2)-> p1.getId().compareTo(p2.getId());

public class PartitionComparator implements Comparator<Partition>, Serializable {

	private static final long serialVersionUID = 3835884999497299152L;

	@Override
    public int compare(Partition p1, Partition p2) {
        return p1.getId().compareTo(p2.getId());
    }
}