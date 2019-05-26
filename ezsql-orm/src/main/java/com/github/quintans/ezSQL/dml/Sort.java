package com.github.quintans.ezSQL.dml;


import com.github.quintans.ezSQL.db.Column;

public class Sort {
	private String alias;
	private ColumnHolder columnHolder;
	private boolean asc;

	private Sort(Builder builder) {
		alias = builder.alias;
		columnHolder = builder.columnHolder;
		asc = builder.asc;
	}

	public static Sort asc(String alias) {
		return new Sort(alias, true);
	}

	public static Sort desc(String alias) {
		return new Sort(alias, false);
	}

	public static Sort asc(Column column) {
		return new Sort(ColumnHolder.of(column), true);
	}

	public static Sort desc(Column column) {
		return new Sort(ColumnHolder.of(column), false);
	}

	public Sort(ColumnHolder column, boolean asc) {
		this.columnHolder = column;
		this.asc = asc;
	}

	public Sort(String alias) {
		this(alias, true);
	}

	public Sort(String alias, boolean asc) {
		this.alias = alias;
		this.asc = asc;
	}

	public static Builder builder() {
		return new Builder();
	}

	public Builder toBuilder() {
		Builder builder = new Builder();
		builder.alias = alias;
		builder.columnHolder = (ColumnHolder) columnHolder.clone();
		builder.asc = asc;
		return builder;
	}

	public String getAlias() {
		return this.alias;
	}

	public ColumnHolder getColumnHolder() {
		return this.columnHolder;
	}

	public boolean isAsc() {
		return this.asc;
	}

	public static final class Builder {
		private String alias;
		private ColumnHolder columnHolder;
		private boolean asc;

		private Builder() {
		}

		public Builder alias(String alias) {
			this.alias = alias;
			return this;
		}

		public Builder columnHolder(ColumnHolder columnHolder) {
			this.columnHolder = columnHolder;
			return this;
		}

		public Builder asc(boolean asc) {
			this.asc = asc;
			return this;
		}

		public Sort build() {
			return new Sort(this);
		}
	}
}
