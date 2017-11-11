package com.github.quintans.ezSQL.orm.extended;

import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.dml.ColumnHolder;
import com.github.quintans.ezSQL.dml.Condition;
import com.github.quintans.ezSQL.dml.EFunction;

public class FunctionExt extends EFunction {
	// DECLARATIONS
	public static final String IFNULL = "IFNULL";
	
	// FACTORIES
    public static Condition ifNull(Column<?> column, Object value) {
    	return new Condition(IFNULL, new ColumnHolder(column), value);
    }
}
