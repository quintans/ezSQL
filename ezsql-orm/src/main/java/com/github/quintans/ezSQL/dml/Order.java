package com.github.quintans.ezSQL.dml;


public class Order {
	private String alias;
	private ColumnHolder column;
	private boolean asc;

	public Order(ColumnHolder column) {
		this(column, true);
	}

	public Order(ColumnHolder column, boolean asc) {
		this.column = column;
		this.asc = asc;
	}

	public Order(String alias) {
		this(alias, true);
	}

	public Order(String alias, boolean asc) {
		this.alias = alias;
		this.asc = asc;
	}

	public String getAlias() {
		return this.alias;
	}

	public ColumnHolder getHolder() {
		return this.column;
	}

	public boolean isAsc() {
		return this.asc;
	}

	public void setAsc(boolean asc) {
		this.asc = asc;
	}

}
