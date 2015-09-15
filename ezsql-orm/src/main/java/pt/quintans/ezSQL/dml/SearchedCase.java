package pt.quintans.ezSQL.dml;

import java.util.ArrayList;
import java.util.List;

/**
 * Searched CASE Expression
 * ------------------------
 *
 * The syntax for a searched CASE expression is:
 *
 * SELECT CASE
 *  WHEN "condition1" THEN "result1"
 *  WHEN "condition2" THEN "result2"
 *  ...
 *  [ELSE "resultN"]
 *  END
 * FROM "table_name";
 *
 * @author paulo
 *
 */
public class SearchedCase {
	public static class SearchedWhen {
		private SearchedCase parent;
		private Condition criteria;
		private Object result;

		public SearchedCase then(Object value) {
			this.result = value;
			return this.parent;
		}
	}
	
	private List<SearchedWhen> whens = new ArrayList<SearchedCase.SearchedWhen>();
	private Object other;

	public SearchedWhen when(Condition criteria) {
		SearchedWhen when = new SearchedWhen();
		when.parent = this;
		when.criteria = criteria;
		this.whens.add(when);
		return when;
	}

	public SearchedCase otherwise(Object value) {
		this.other = value;
		return this;
	}

	public Function end() {
		List<Object> vals = new ArrayList<Object>();
		for (SearchedWhen v : this.whens) {
			vals.add(new Function(EFunction.CASE_WHEN, v.criteria, v.result));
		}
		if (this.other != null) {
			vals.add(new Function(EFunction.CASE_ELSE, this.other));
		}
		return new Function(EFunction.CASE, vals.toArray());
	}
	
}
