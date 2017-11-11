package com.github.quintans.ezSQL.dml;

import com.github.quintans.ezSQL.db.Column;

/**
 * User: quintans
 * Date: 12-12-2007
 * Time: 2:01:24
 */
public class ColumnHolder extends Function {
	// can be a Column, a Function or any constante
	protected Column<?> column;

	public ColumnHolder(Column<?> column) {
		this.operator = EFunction.COLUMN;
		this.value = column;
		this.column = column;
	}

    public ColumnHolder of(String tableAlias) {
        this.tableAlias = tableAlias;
        return this;
    }
    
    public String getAlias() {
        return this.alias != null ? this.alias : column.getAlias();
    }
    
	@Override
	public void setTableAlias(String tableAlias) {
		if (this.tableAlias == null) {
			this.tableAlias = tableAlias;
		}
	}

	public Column<?> getColumn() {
		return this.column;
	}

	@Override
	public String toString() {
		return String.format("%s.%s%s",
			this.tableAlias != null ? this.tableAlias : this.column.getTable().getName(),
			this.column.getName(),
			this.alias != null ? " " + this.alias : "");
	}

	@Override
	public Object clone() {
	    ColumnHolder ch = new ColumnHolder(this.column);
	    ch.alias = this.alias;
	    ch.tableAlias = tableAlias;
	    return ch;
	}

}