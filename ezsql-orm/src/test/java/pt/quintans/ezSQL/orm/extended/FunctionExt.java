package pt.quintans.ezSQL.orm.extended;

import pt.quintans.ezSQL.db.Column;
import pt.quintans.ezSQL.dml.ColumnHolder;
import pt.quintans.ezSQL.dml.Condition;
import pt.quintans.ezSQL.dml.EFunction;

public class FunctionExt extends EFunction {
	// DECLARATIONS
	public static final String IFNULL = "IFNULL";
	
	// FACTORIES
    public static Condition ifNull(Column<?> column, Object value) {
    	return new Condition(IFNULL, new ColumnHolder(column), value);
    }
}
