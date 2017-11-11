package com.github.quintans.ezSQL.dml;

public class Union {
	private Query query;
	private boolean all;
	
	public Union(Query query, boolean all) {
		this.query = query;
		this.all = all;
	}

	public Query getQuery() {
		return query;
	}
	public boolean isAll() {
		return all;
	}	
	
	

}
