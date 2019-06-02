package com.github.quintans.ezSQL.translator;

import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.dml.QueryDSL;
import com.github.quintans.ezSQL.exception.OrmException;

public class HSQLDBTranslator extends GenericTranslator {

  public String getAutoNumberQuery(Column<? extends Number> column, boolean current) {
    if (column.isKey())
      return "call identity()";
    else
      throw new OrmException("The function getAutoNumberQuery does not recognizes the column %s", column);
  }

  @Override
  public String paginate(QueryDSL query, String sql) {
    return sql;
  }

}
