package com.github.quintans.ezSQL.translator;

import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.dml.Function;
import com.github.quintans.ezSQL.dml.QueryDSL;
import com.github.quintans.ezSQL.dml.UpdateDSL;
import com.github.quintans.ezSQL.exception.OrmException;

public class PostgreSQLTranslator extends GenericTranslator {
  @Override
  public String getAutoNumberQuery(Column<? extends Number> column, boolean current) {
    if (column.isKey())
      return String.format("SELECT %s('%s_%s_seq');", current ? "currval" : "nextval", tableName(column.getTable()), columnName(column));
    else
      throw new OrmException("column '%s' must be key.", column);
  }

  @Override
  public boolean useSQLPagination() {
    return true;
  }

  @Override
  public String paginate(QueryDSL query, String sql) {
    StringBuilder sb = new StringBuilder();
    if (query.getLimit() > 0) {
      sb.append(sql).append(" LIMIT :").append(QueryDSL.LAST_RESULT);
      query.setParameter(QueryDSL.LAST_RESULT, query.getLimit());
      if (query.getSkip() > 0) {
        sb.append(" OFFSET :").append(QueryDSL.FIRST_RESULT);
        query.setParameter(QueryDSL.FIRST_RESULT, query.getSkip());
      }
      return sb.toString();
    }

    return sql;
  }

  @Override
  public boolean ignoreNullKeys() {
    return true;
  }

  @Override
  public String tableName(Table table) {
    return table.getName().toLowerCase();
  }

  @Override
  public String columnName(Column<?> column) {
    return column.getName().toLowerCase();
  }

  @Override
  public UpdateBuilder createUpdateBuilder(UpdateDSL update) {
    return new GenericUpdateBuilder(update) {
      @Override
      public void column(Column<?> column, Function token) {
        this.columnPart.addAsOne(
            driver().columnName(column),
            " = ", driver().translate(EDml.UPDATE, token));
      }
    };
  }

}
