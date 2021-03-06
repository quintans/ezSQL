package com.github.quintans.ezSQL.driver;

import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.NullSql;
import com.github.quintans.ezSQL.db.Sequence;
import com.github.quintans.ezSQL.dml.AutoKeyStrategy;
import com.github.quintans.ezSQL.dml.ColumnHolder;
import com.github.quintans.ezSQL.dml.Condition;
import com.github.quintans.ezSQL.dml.Definition;
import com.github.quintans.ezSQL.dml.DeleteDSL;
import com.github.quintans.ezSQL.dml.EFunction;
import com.github.quintans.ezSQL.dml.Function;
import com.github.quintans.ezSQL.dml.Join;
import com.github.quintans.ezSQL.dml.PathElement;
import com.github.quintans.ezSQL.dml.QueryDSL;
import com.github.quintans.ezSQL.dml.UpdateDSL;
import com.github.quintans.jdbc.PreparedStatementCallback;
import com.github.quintans.jdbc.exceptions.PersistenceException;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class OracleDriver extends GenericDriver {
  /**
   * maximum length for object names in Oracle
   */
  private static final int NAME_MAX_LEN = 30;

  @Override
  public AutoKeyStrategy getAutoKeyStrategy() {
    return AutoKeyStrategy.BEFORE;
  }

  @Override
  public String getAutoNumberQuery(Column<? extends Number> column, boolean current) {
    if (column.isKey())
      return "select S_" + column.getTable().getName() + (current ? ".CURRVAL" : ".NEXTVAL") + " from DUAL";
    else
      throw new PersistenceException(String.format("A função getAutoNumberQuery não reconhece a coluna %s.", column));
  }

  @Override
  public String getSql(Sequence sequence, boolean nextValue) {
    return String.format("select %s.%s from DUAL", sequence.getName().toUpperCase(), (nextValue ? "NEXTVAL" : "CURRVAL"));
  }

  @Override
  protected String getDefault() {
    return "null";
  }

  @Override
  public int paginationColumnOffset(QueryDSL query) {
    if (useSQLPagination() && query.getSkip() >= 1)
      return 1;
    else
      return 0;
  }

  private boolean sqlPagination = true;

  public void setUseSQLPagination(boolean sqlPagination) {
    this.sqlPagination = sqlPagination;
  }

  @Override
  public boolean useSQLPagination() {
    return this.sqlPagination;
  }

  @Override
  public String autoNumber(EDml dmlType, Function function) {
    Object[] o = function.getMembers();
    Column<?> column = ((ColumnHolder) o[0]).getColumn();
    if (column.isKey())
      return String.format("S_SGNID.NEXTVAL");
    else if (column.isDeletion())
      return String.format("S_SGNSTATUSREGISTO.NEXTVAL");
    else
      throw new PersistenceException(String.format("A operação autonumber não reconhece a coluna %s.", column));
  }

  @Override
  public String secondsdiff(EDml dmlType, Function function) {
    Object[] o = function.getMembers();
    // DEVERIA INVERTER: - ( %s )
    return String.format("(SYSDATE - ( %s ) - SYSDATE)*86400", rolloverParameter(dmlType, o, " - "));
  }

  @Override
  public String now(EDml dmlType, Function function) {
    return "SYSDATE";
  }

  @Override
  public String paginate(QueryDSL query, String sql) {
    if (query.getSkip() > 0) { // se o primeiro resultado esta definido o ultimo tb esta
      // return String.format("select * from	( select rownum rnum, a.* from ( %s ) a where rownum <= %s ) where rnum >= %s",
      // sql, query.getFirstResult() + query.getMaxResults() - 1, query.getFirstResult());
      query.setParameter(QueryDSL.FIRST_RESULT, query.getSkip() + 1);
      query.setParameter(QueryDSL.LAST_RESULT, query.getSkip() + query.getLimit());
      return String.format("select * from	( select rownum rnum, a.* from ( %s ) a where rownum <= :%s ) where rnum >= :%s",
          sql, QueryDSL.LAST_RESULT, QueryDSL.FIRST_RESULT);
    } else if (query.getLimit() > 0) {
      // return String.format("select * from ( %s ) where rownum <= %s", sql, query.getMaxResults());
      query.setParameter(QueryDSL.LAST_RESULT, query.getLimit());
      return String.format("select * from ( %s ) where rownum <= :%s", sql, QueryDSL.LAST_RESULT);
    } else
      return sql;
  }

  @Override
  public String columnAlias(Function function, int position) {
    String alias = function.getAlias();
    if (alias == null) {
      if (function instanceof ColumnHolder) {
        ColumnHolder ch = (ColumnHolder) function;
        alias = ch.getTableAlias() + "_" + ch.getColumn().getName();
      } else if (!EFunction.ALIAS.equals(function.getOperator())) {
        alias = function.getOperator() + "_" + position;
      }
    } else {
      alias = super.columnAlias(function, position);
    }

    if (alias.length() > NAME_MAX_LEN) {
      alias = alias.substring(0, NAME_MAX_LEN - 3) + position; // use of position to avoid name collisions
    }

    return alias;
  }

  @Override
  public Boolean toBoolean(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    Object o = rs.getObject(columnIndex);
    return (rs.wasNull() ? null : "1".equals(o));
  }

  @Override
  public Date toTimestamp(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    Timestamp o = rs.getTimestamp(columnIndex, getCalendar());
    return (rs.wasNull() ? null : new Date(o.getTime()));
  }

  @Override
  public Object fromBoolean(Boolean o) {
    if (o != null)
      return o ? "1" : "0";
    else
      return NullSql.CHAR;
  }

  @Override
  public Object fromTimestamp(final java.util.Date o) {
    if (o == null)
      return NullSql.DATE;
    else
      return new PreparedStatementCallback() {
        @Override
        public void execute(PreparedStatement ps, int columnIndex) throws SQLException {
          ps.setTimestamp(columnIndex, new java.sql.Timestamp(o.getTime()), getCalendar());
        }
      };
  }

  @Override
  protected Object toDefault(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    int sqlType = rsw.getSqlType(columnIndex);

    Object o = null;

    switch (sqlType) {
      case Types.TIME:
        o = rs.getTime(columnIndex);
        break;

      case Types.DATE:
        o = rs.getDate(columnIndex);
        break;

      case Types.TIMESTAMP:
        o = rs.getTimestamp(columnIndex, getCalendar());
        break;

      case Types.CHAR:
        o = rs.getString(columnIndex);
        if ("0".equals(o))
          o = false;
        else if ("1".equals(o))
          o = true;
        break;

      default:
        o = rs.getObject(columnIndex);
        break;

    }

    if (rs.wasNull())
      return null;
    else
      return o;
  }

  @Override
  public String getSql(UpdateDSL update) {
    // with joins - http://stackoverflow.com/questions/2446764/oracle-update-statement-with-inner-join
    /*
     * // update to a value from the table at the end of the inner join
     * UPDATE employee e
     * SET e.BONUS = b."VALUE"
     * INNER JOIN bonus b ON e.ID = b.EMPLOYEEID
     * ---------->
     * UPDATE (
     * SELECT e.BONUS val1, b."VALUE" as val2
     * FROM employee e
     * INNER JOIN bonus b
     * ON e.ID = b.EMPLOYEEID
     * )
     * SET val1 = val2;
     */
    if (update.getJoins() != null) {
      StringBuilder set = new StringBuilder();
      StringBuilder sb = new StringBuilder();
      sb.append("update (");

      QueryDSL query = new QueryDSL(this, update.getTable());
      Map<Column<?>, Function> values = update.getValues();
      int idx = 1;
      for (Entry<Column<?>, Function> entry : values.entrySet()) {
        String key = "key_" + idx;
        String val = "val_" + idx;
        query.column(entry.getKey()).as(key);
        query.column(entry.getValue()).as(val);
        if (idx > 1)
          set.append(", ");
        set.append(key).append(" = ").append(val);
        idx++;
      }
      for (Join join : update.getJoins()) {
        for (PathElement pathElement : join.getPathElements()) {
          if (pathElement.isInner())
            query.inner(pathElement.getBase());
          else
            query.outer(pathElement.getBase());
        }
        query.join();
      }
      if (update.getCondition() != null)
        query.where(update.getCondition());

      sb.append(getSql(query))
          .append(") set ").append(set.toString());

      return sb.toString();
    } else {
      return super.getSql(update);
    }
  }

  // DELETE
  @Override
  public String getSql(DeleteDSL delete) {
    String sql = super.getSql(delete);
    /*
     * delete from employee e
     * where exists (
     * SELECT b.EMPLOYEEID FROM EMPLOYEE x INNER JOIN Bonus b ON x.ID = b.EMPLOYEEID
     * where e.ID = x.ID
     * );
     * --> if foreign keys are in place it will fail :p
     */
    if (delete.getJoins() != null) {
      StringBuilder sb = new StringBuilder(sql);

      if (delete.getCondition() != null)
        sb.append(" and ");
      else
        sb.append(" where ");

      sb.append("exists (");

      String alias = delete.getTable().getAlias() + "_i";
      QueryDSL query = new QueryDSL(this, delete.getTable()).as(alias)
          .column(Definition.asIs(1L));
      for (Join join : delete.getJoins()) {
        for (PathElement pathElement : join.getPathElements()) {
          if (pathElement.isInner())
            query.inner(pathElement.getBase());
          else
            query.outer(pathElement.getBase());
        }
        query.join();
      }

      // joining inner query with the delete table
      List<Condition> conditions = new ArrayList<>();
      for (Column<?> column : delete.getTable().getKeyColumns()) {
        conditions.add(column.is(column.of(alias)));
      }
      query.where(conditions);

      sb.append(getSql(query)).append(")");

      return sb.toString();
    } else {
      return sql;
    }
  }

}
