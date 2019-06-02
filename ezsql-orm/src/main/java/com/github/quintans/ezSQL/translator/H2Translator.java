package com.github.quintans.ezSQL.translator;

import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.dml.ColumnHolder;
import com.github.quintans.ezSQL.dml.EFunction;
import com.github.quintans.ezSQL.dml.Function;
import com.github.quintans.ezSQL.dml.QueryDSL;
import com.github.quintans.ezSQL.exception.OrmException;

public class H2Translator extends GenericTranslator {

  @Override
  public String getAutoNumberQuery(Column<? extends Number> column, boolean current) {
    if (column.isKey())
      return "call identity()";
    else
      throw new OrmException("The function getAutoNumberQuery does not recognizes the column %s", column);
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
  public String paginate(QueryDSL query, String sql) {
    StringBuilder sb = new StringBuilder();
    if (query.getLimit() > 0) {
      sb.append(" LIMIT :").append(QueryDSL.LAST_RESULT);
      query.setParameter(QueryDSL.LAST_RESULT, query.getLimit());
      if (query.getSkip() > 0) {
        sb.append(" OFFSET :").append(QueryDSL.FIRST_RESULT);
        query.setParameter(QueryDSL.FIRST_RESULT, query.getSkip());
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

}
