package com.github.quintans.ezSQL.driver;

import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.dml.AutoKeyStrategy;
import com.github.quintans.ezSQL.dml.ColumnHolder;
import com.github.quintans.ezSQL.dml.EFunction;
import com.github.quintans.ezSQL.dml.Function;
import com.github.quintans.ezSQL.dml.Query;
import com.github.quintans.jdbc.exceptions.PersistenceException;

public class H2Driver extends GenericDriver {

	@Override
	public String getAutoNumberQuery(Column<? extends Number> column, boolean current) {
		if (column.isKey())
			return "call identity()";
		else
			throw new PersistenceException(String.format("A função getAutoNumberQuery não reconhece a coluna %s.", column));
	}

	@Override
	public boolean useSQLPagination() {
		return true;
	}

	@Override
	public String secondsdiff(EDml dmlType, Function function) {
		Object[] o = function.getMembers();
		return String.format("DATEDIFF('SS', %s)", rolloverParameter(dmlType, o, ", "));
	}

	@Override
	public String paginate(Query query, String sql) {
		StringBuilder sb = new StringBuilder();
		if (query.getLimit() > 0) {
			sb.append(" LIMIT :").append(Query.LAST_RESULT);
			query.setParameter(Query.LAST_RESULT, query.getLimit());
			if (query.getSkip() > 0) {
				sb.append(" OFFSET :").append(Query.FIRST_RESULT);
				query.setParameter(Query.FIRST_RESULT, query.getSkip());
			}
		}

		return String.format("%s%s", sql, sb.toString());
	}	
	
    @Override
	public String columnAlias(Function function, int position) {
		String alias = function.getAlias();
		if (alias == null) {
			if (function instanceof ColumnHolder) {
				ColumnHolder ch = (ColumnHolder) function;
				alias = ch.getTableAlias() + "_" + ch.getColumn().getName();
			} else if (!EFunction.ALIAS.equals(function.getOperator()))
				alias = function.getOperator() + "_" + position;
		} else {
			alias = super.columnAlias(function, position);
		}

		return alias;
	}

    @Override
    public AutoKeyStrategy getAutoKeyStrategy() {
        return AutoKeyStrategy.RETURNING;
    }
	
}
