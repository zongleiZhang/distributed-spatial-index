package com.ada.QBSTree;

import com.ada.common.Path;

import java.io.Serializable;

@SuppressWarnings("rawtypes")
public class CacheElem implements Serializable{
	public RCDataNode leaf;
	public Path path;
	
	public CacheElem() {}

	CacheElem(RCDataNode leaf) {
		super();
		this.leaf = leaf;
		this.path = new Path(leaf);
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof CacheElem) {
			CacheElem cur = (CacheElem) obj;
			return cur.leaf.equals(this.leaf);
		}else {
			return false;
		}
	}
	
}
