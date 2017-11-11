package com.github.quintans.ezSQL.dml;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.github.quintans.ezSQL.db.ColGroup;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.dml.SearchedCase.SearchedWhen;
import com.github.quintans.ezSQL.exceptions.PersistenceException;
import com.github.quintans.ezSQL.toolkit.utils.Misc;

public class Definition {
    public static ColGroup ASSOCIATE(Column<?>... from) {
        // all columns must belong to the same table
        if (Misc.length(from) > 0) {
            Table table = from[0].getTable();
            for (Column<?> source : from) {
                if (!table.equals(source.getTable())) {
                    throw new PersistenceException("All columns must belong to the same table");
                }
            }
            return new ColGroup(from);
        }

        return null;
    }   
    
	// CONDITION ===========================
	public static Condition or(List<Condition> operations) {
		if (operations.size() == 1)
			return operations.get(0);

		return or(operations.toArray(new Condition[0]));
	}

	public static Condition or(Condition... operations) {
		if (operations.length == 1)
			return operations[0];

		return new Condition(EFunction.OR, (Object[]) operations);
	}

	public static Condition and(List<Condition> operations) {
		if (operations.size() == 1)
			return operations.get(0);

		return and(operations.toArray(new Condition[0]));
	}

	public static Condition and(Condition... operations) {
		if (operations.length == 1)
			return operations[0];

		return new Condition(EFunction.AND, (Object[]) operations);
	}

	// conditions for columns
	public static Condition greater(Object column, Object value) {
		return new Condition(EFunction.GT, column, value);
	}

	public static Condition greaterOrMatch(Object column, Object value) {
		return new Condition(EFunction.GTEQ, column, value);
	}

	public static Condition lesser(Object column, Object value) {
		return new Condition(EFunction.LT, column, value);
	}

	public static Condition lesserOrMatch(Object column, Object value) {
		return new Condition(EFunction.LTEQ, column, value);
	}

	public static Condition is(Object left, Object right) {
		return new Condition(EFunction.EQ, left, right);
	}
	
	public static Condition range(Object column, Object bottom, Object top) {
		if (bottom != null && top != null)
			return new Condition(EFunction.RANGE, column, bottom, top);
		else if (bottom != null)
			return greaterOrMatch(column, bottom);
		else if (top != null)
			return lesserOrMatch(column, top);
		else
			throw new PersistenceException("Invalid Range Function");
	}

	public static Condition valueRange(Object bottom, Object top, Object value) {
		return new Condition(EFunction.VALUERANGE, bottom, top, value);
	}

	public static Condition boundedRange(Object bottom, Object top, Object value) {
		return new Condition(EFunction.BOUNDEDRANGE, bottom, top, value);
	}

	public static Condition isNull(Object column) {
		return new Condition(EFunction.ISNULL, column);
	}

	public static Condition in(Object column, Collection<?> values) {
		List<Object> vals = new ArrayList<Object>(values.size() + 1);
		vals.add(column);
		vals.addAll(values);
		return new Condition(EFunction.IN, vals.toArray(new Object[vals.size()]));
	}

	public static Condition in(Object column, Object... value) {
	    Object[] fs = new Object[value.length + 1];
		fs[0] = column;
		System.arraycopy(value, 0, fs, 1, value.length);
		return new Condition(EFunction.IN, fs);
	}

	public static Condition iMatches(Object left, Object right) {
		return new Condition(EFunction.IEQ, left, right);
	}

	public static Condition like(Object left, Object right) {
		return new Condition(EFunction.LIKE, left, right);
	}

	public static Condition iLike(Object left, Object right) {
		return new Condition(EFunction.ILIKE, left, right);
	}

	public static Condition different(Object left, Object right) {
		return new Condition(EFunction.NEQ, left, right);
	}

	public static Condition exists(Object o) {
		return new Condition(EFunction.EXISTS, o); // EXISTS info
	}

	public static Condition not(Object o) {
		return new Condition(EFunction.NOT, o); // NOT info
	}

	/*===============================================
	 * FUNCTIONS
	 ================================================*/

	public static Function param(String o) {
		return new FunctionEnd(EFunction.PARAM, o);
	}

    public static Function param(Column<?> c) {
        return new FunctionEnd(EFunction.PARAM, c.getAlias());
    }

	public static Function raw(Object o) {
		return new FunctionEnd(EFunction.RAW, o); // RAW info
	}

	public static Function asIs(Object o) {
		return new FunctionEnd(EFunction.ASIS, o); // AS IS info
	}

	public static Function as(String o) {
		return new FunctionEnd(EFunction.ALIAS, o);
	}

	public static Function sum(Object o) {
		return new Function(EFunction.SUM, o);
	}

	public static Function max(Object o) {
		return new Function(EFunction.MAX, o);
	}

	public static Function min(Object o) {
		return new Function(EFunction.MIN, o);
	}

	public static Function count() {
		return new Function(EFunction.COUNT);
	}

	public static Function count(Object o) {
		return new Function(EFunction.COUNT, o);
	}

	public static Function rtrim(Object o) {
		return new Function(EFunction.RTRIM, o);
	}

	public static Function now() {
		return new FunctionEnd(EFunction.NOW, null);
	}

	// os elementos podem ser colunas ou funcoes
	public static Function add(Object... o) {
		return new Function(EFunction.ADD, o);
	}

	// os elementos podem ser colunas ou funcoes
	public static Function minus(Object... o) {
		return new Function(EFunction.MINUS, o);
	}

	// os elementos podem ser colunas ou funcoes
	public static Function multiply(Object... o) {
		return new Function(EFunction.MULTIPLY, o);
	}

	public static Function secondsdiff(Object... o) {
		return new Function(EFunction.SECONDSDIFF, o);
	}

	public static <T extends Number> Function autoNumber(Column<T> o) {
		return new Function(EFunction.AUTONUM, o);
	}

	public static Function subQuery(Query inner) {
		return new FunctionEnd(EFunction.SUBQUERY, inner);
	}
	
    public static Function upper(Object o) {
        return new Function(EFunction.UPPER, o);
    }
    public static Function lower(Object o) {
        return new Function(EFunction.LOWER, o);
    }
    public static Function coalesc(Object... o) {
        return new Function(EFunction.COALESCE, o);
    }
	
    /**
     * Searched CASE
     * 
     * @param criteria
     * @return
     */
    public static SearchedWhen when(Condition criteria) {
    	return new SearchedCase().when(criteria);
    }

    /**
     * Simple CASE
     * 
     * @param expression
     * @return
     */
    public static SimpleCase with(Object expression) {
    	return new SimpleCase(expression);
    }
}
