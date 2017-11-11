package com.github.quintans.ezSQL.dml;

import java.util.ArrayList;
import java.util.List;

/**
 * Simple CASE Expression
 * ----------------------
 *
 * The syntax for a simple CASE expression is:
 *
 * SELECT CASE ("column_name")
 *  WHEN "value1" THEN "result1"
 *  WHEN "value2" THEN "result2"
 *  ...
 *  [ELSE "resultN"]
 *  END
 * FROM "table_name";
 *
 * @author paulo
 *
 */
public class SimpleCase {
	public static class SimpleWhen {
		private SimpleCase parent;
		private Object expression;
		private Object result;

		public SimpleCase then(Object value) {
			this.result = value;
			return this.parent;
		}

	}

	private Object expression;
	private List<SimpleWhen> whens = new ArrayList<SimpleCase.SimpleWhen>();      
	private Object other;
	
	public SimpleCase(Object expression) {
		this.expression = expression;
	}

	public SimpleWhen when(Object expression) {
		SimpleWhen when = new SimpleWhen();
		when.parent = this;
		when.expression = expression;
		this.whens.add(when);
		return when;
	}

	public SimpleCase otherwise(Object value) {
		this.other = value;
		return this;
	}

	public Function end() {
		List<Object> vals = new ArrayList<Object>();
		if (this.expression != null) {
			vals.add(this.expression);
		}
		for (SimpleWhen v : this.whens) {
			vals.add(new Function(EFunction.CASE_WHEN, v.expression, v.result));
		}
		if (this.other != null) {
			vals.add(new Function(EFunction.CASE_ELSE, this.other));
		}
		return new Function(EFunction.CASE, vals.toArray());
	}
	
}
