package com.smx.spark.bio.part;

import java.io.Serializable;

public class Partition implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6676464626191423766L;
	private Integer id = 0;
	private String fileName;
	
	public Partition() {
	}
	
	public Partition(Integer id) {
		this.id = id;
	}
	
	public Partition(Integer id, String fileName) {
		this.id = id;
		this.fileName = fileName;
	}
	
	public Integer getId() {
		return id;
	}
	
	public String getFileName() {
		return fileName;
	}
	
    @Override
    public String toString() {
        
        String v = "";
        if (this.fileName != null) {
            v = this.fileName.toString();
        }
            
        return "(" + id + " => " + v + ")";
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        
        if (other == null) {
            return false;
        }
        
        if (! other.getClass().isInstance(this)) {
            return false;
        }
        
        return this.id.equals(((Partition) other).id);
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 83 * hash + (this.id != null ? this.id.hashCode() : 0);
        return hash;
    }  
}
