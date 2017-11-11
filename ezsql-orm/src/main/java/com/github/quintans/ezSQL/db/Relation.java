package com.github.quintans.ezSQL.db;

import com.github.quintans.ezSQL.dml.ColumnHolder;


public class Relation {
	private ColumnHolder from;
	private ColumnHolder to;
	
	public Relation(Column<?> from, Column<?> to) {
		super();
		this.from = new ColumnHolder(from);
		this.to = new ColumnHolder(to);
	}

	public ColumnHolder getFrom() {
		return from;
	}

	public ColumnHolder getTo() {
		return to;
	}
	
	public String toString(){
		return from.toString() + " -> " + to.toString();
	}

	public Relation bareCopy(){
		return new Relation(from.getColumn(), to.getColumn());
	}

}
