package com.github.quintans.ezSQL.translator;

import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.dml.DeleteDSL;
import com.github.quintans.ezSQL.dml.QueryDSL;
import com.github.quintans.ezSQL.exception.OrmException;
import com.github.quintans.ezSQL.sp.SqlProcedureDSL;

public class MySQLTranslator extends GenericTranslator {
  private final String DATE_FORMAT = "yyyy-MM-dd";
  private final String TIME_FORMAT = "HH:mm:ss";
  private final String DATETIME_FORMAT = DATE_FORMAT + "'T'" + TIME_FORMAT;
  private final String TIMESTAMP_FORMAT = DATETIME_FORMAT + ".SSS";
  private final String ISO_FORMAT = TIMESTAMP_FORMAT + " zzz";
  //private final String ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS zzz";
  private String TIME_ZONE = "UTC";

  public MySQLTranslator() {
    super();
  }

  @Override
  public String getAutoNumberQuery(Column<? extends Number> column, boolean current) {
    if (column.isKey())
      return "SELECT LAST_INSERT_ID()";
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
        sb.append(", :").append(QueryDSL.FIRST_RESULT);
        query.setParameter(QueryDSL.FIRST_RESULT, query.getSkip());
      }
      return sb.toString();
    }

    return sql;
  }

  @Override
  public String tableName(Table table) {
    return "`" + table.getName().toUpperCase() + "`";
  }

  @Override
  public String columnName(Column<?> column) {
    return "`" + column.getName().toUpperCase() + "`";
  }

  @Override
  public String procedureName(SqlProcedureDSL procedure) {
    return "`" + procedure.getName() + "`";
  }

  @Override
  protected DeleteBuilder createDeleteBuilder(DeleteDSL delete) {
    return new GenericDeleteBuilder(delete) {

      @Override
      public void from() {
        Table table = delete.getTable();
        String alias = delete.getTableAlias();
        tablePart.addAsOne(alias, " USING ", this.driver().tableName(table), " AS ", alias);
      }

    };
  }

}
