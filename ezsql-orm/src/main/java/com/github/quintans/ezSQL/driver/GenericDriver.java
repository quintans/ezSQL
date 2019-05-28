package com.github.quintans.ezSQL.driver;

import com.github.quintans.ezSQL.common.api.Converter;
import com.github.quintans.ezSQL.common.api.Value;
import com.github.quintans.ezSQL.common.type.MyDate;
import com.github.quintans.ezSQL.common.type.MyDateTime;
import com.github.quintans.ezSQL.common.type.MyTime;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.NullSql;
import com.github.quintans.ezSQL.db.Sequence;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.dml.ColumnHolder;
import com.github.quintans.ezSQL.dml.Condition;
import com.github.quintans.ezSQL.dml.DeleteDSL;
import com.github.quintans.ezSQL.dml.EFunction;
import com.github.quintans.ezSQL.dml.Function;
import com.github.quintans.ezSQL.dml.InsertDSL;
import com.github.quintans.ezSQL.dml.QueryDSL;
import com.github.quintans.ezSQL.dml.UpdateDSL;
import com.github.quintans.ezSQL.jdbc.AbstractPreparedStatementCallback;
import com.github.quintans.ezSQL.sp.SqlProcedure;
import com.github.quintans.ezSQL.toolkit.io.AutoCloseInputStream;
import com.github.quintans.ezSQL.toolkit.io.BinStore;
import com.github.quintans.ezSQL.toolkit.io.TextStore;
import com.github.quintans.ezSQL.toolkit.utils.Misc;
import com.github.quintans.ezSQL.mapper.DeleteMapper;
import com.github.quintans.ezSQL.mapper.DeleteMapperBean;
import com.github.quintans.ezSQL.mapper.InsertMapper;
import com.github.quintans.ezSQL.mapper.InsertMapperBean;
import com.github.quintans.ezSQL.mapper.MapperSupporter;
import com.github.quintans.ezSQL.mapper.QueryMapper;
import com.github.quintans.ezSQL.mapper.QueryMapperBean;
import com.github.quintans.ezSQL.mapper.UpdateMapper;
import com.github.quintans.ezSQL.mapper.UpdateMapperBean;
import com.github.quintans.jdbc.exceptions.PersistenceException;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import static com.github.quintans.ezSQL.toolkit.utils.Misc.length;

/*
 * NOTA:
 * Sá as colunas da tabela principal estão com alias gerado
 */
public abstract class GenericDriver implements Driver {
  private Collection<QueryMapper> queryMappers;
  private Collection<InsertMapper> insertMappers;
  private Collection<DeleteMapper> deleteMappers;
  private Collection<UpdateMapper> updateMappers;
  private ConcurrentHashMap<Class<? extends Converter>, Converter> converters;
  private Calendar calendar = Calendar.getInstance();

  public GenericDriver() {
    this.queryMappers = new ConcurrentLinkedDeque<>();
    this.queryMappers.add(new QueryMapperBean());
    this.insertMappers = new ConcurrentLinkedDeque<>();
    this.insertMappers.add(new InsertMapperBean());
    this.deleteMappers = new ConcurrentLinkedDeque<>();
    this.deleteMappers.add(new DeleteMapperBean());
    this.updateMappers = new ConcurrentLinkedDeque<>();
    this.updateMappers.add(new UpdateMapperBean());
    this.converters = new ConcurrentHashMap<>();
  }

  public void setTimeZoneId(String tzId) {
    this.calendar = Calendar.getInstance(TimeZone.getTimeZone(tzId));
  }

  public Calendar getCalendar() {
    return this.calendar;
  }

  @Override
  public String getAutoNumberQuery(Column<? extends Number> column) {
    return getAutoNumberQuery(column, false);
  }

  @Override
  public String getCurrentAutoNumberQuery(Column<? extends Number> column) {
    return getAutoNumberQuery(column, true);
  }

  public String getAutoNumberQuery(Column<? extends Number> column, boolean current) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isPmdKnownBroken() {
    return false;
  }

  @Override
  public Object transformParameter(Object parameter) {
    Object val = parameter;
    if (val instanceof NullSql) {
      val = this.fromNull((NullSql) val);
    } else if (val instanceof Boolean) {
      val = this.fromBoolean((Boolean) val);
    } else if (val instanceof Byte) {
      val = this.fromTiny((Byte) val);
    } else if (val instanceof Short) {
      val = this.fromShort((Short) val);
    } else if (val instanceof Integer) {
      val = this.fromInteger((Integer) val);
    } else if (val instanceof Long) {
      val = this.fromLong((Long) val);
    } else if (val instanceof Double) {
      val = this.fromDecimal((Double) val);
    } else if (val instanceof BigDecimal) {
      val = this.fromBigDecimal((BigDecimal) val);
    } else if (val instanceof String) {
      val = this.fromString((String) val);
    } else if (val instanceof MyTime) {
      val = this.fromTime((Date) val);
    } else if (val instanceof MyDate) {
      val = this.fromDate((Date) val);
    } else if (val instanceof MyDateTime) {
      val = this.fromDateTime((Date) val);
    } else if (val instanceof Date) {
      val = this.fromTimestamp((Date) val);
    } else if (val instanceof TextStore) {
      TextStore txt = (TextStore) val;
      try {
        val = this.fromText(txt.getInputStream(), (int) txt.getSize());
      } catch (IOException e) {
        throw new PersistenceException("Unable to get input stream from TextCache!", e);
      }
    } else if (val instanceof BinStore) {
      BinStore bin = (BinStore) val;
      try {
        val = this.fromBin(bin.getInputStream(), (int) bin.getSize());
      } catch (IOException e) {
        throw new PersistenceException("Unable to get input stream from ByteCache!", e);
      }
    } else if (val instanceof char[]) {
      String txt = new String((char[]) val);
      InputStream is;
      try {
        is = IOUtils.toInputStream(txt, TextStore.DEFAULT_CHARSET);
        val = this.fromText(is, txt.length());
      } catch (IOException e) {
        throw new PersistenceException("Unable to get input stream from String!", e);
      }
    } else if (val instanceof byte[]) {
      byte[] bin = (byte[]) val;
      val = this.fromBin(new ByteArrayInputStream(bin), bin.length);
    } else {
      val = this.fromUnknown(val);
    }

    return val;
  }


  @Override
  public Object toIdentity(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    Object o = rs.getObject(columnIndex);
    return (rs.wasNull() ? null : o);
  }

  @Override
  public Boolean toBoolean(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    Boolean o = rs.getBoolean(columnIndex);
    return (rs.wasNull() ? null : o);
  }

  @Override
  public String toString(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    String o = rs.getString(columnIndex);
    return (rs.wasNull() ? null : o);
  }

  @Override
  public Byte toTiny(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    Byte o = rs.getByte(columnIndex);
    return (rs.wasNull() ? null : o);
  }

  @Override
  public Short toShort(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    Short o = rs.getShort(columnIndex);
    return (rs.wasNull() ? null : o);
  }

  @Override
  public Integer toInteger(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    Integer o = rs.getInt(columnIndex);
    return (rs.wasNull() ? null : o);
  }

  @Override
  public Long toLong(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    Long o = rs.getLong(columnIndex);
    return (rs.wasNull() ? null : o);
  }

  @Override
  public Double toDecimal(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    Double o = rs.getDouble(columnIndex);
    return (rs.wasNull() ? null : o);
  }

  @Override
  public BigDecimal toBigDecimal(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    BigDecimal o = rs.getBigDecimal(columnIndex);
    return (rs.wasNull() ? null : o);
  }

  @Override
  public MyTime toTime(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    Date o = rs.getTime(columnIndex);
    return (rs.wasNull() ? null : new MyTime(o.getTime()));
  }

  @Override
  public MyDate toDate(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    Date o = rs.getDate(columnIndex);
    return (rs.wasNull() ? null : new MyDate(o.getTime()));
  }

  @Override
  public MyDateTime toDateTime(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    Date o = rs.getTimestamp(columnIndex);
    return (rs.wasNull() ? null : new MyDateTime(o.getTime()));
  }

  @Override
  public Date toTimestamp(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    Date o = rs.getTimestamp(columnIndex, getCalendar());
    return (rs.wasNull() ? null : o);
  }

  @Override
  public InputStream toText(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    InputStream in = rs.getAsciiStream(columnIndex);
    if (in == null || rs.wasNull()) {
      return null;
    }
    return in;
  }

  @Override
  public InputStream toBin(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    InputStream in = rs.getBinaryStream(columnIndex);
    if (in == null || rs.wasNull()) {
      return null;
    }
    return in;
  }

  public Object fromNull(final NullSql type) {
    return type;
  }

  @Override
  public Object fromIdentity(Object o) {
    return (o == null ? fromNull(NullSql.BIGINT) : o);
  }

  @Override
  public Object fromBoolean(Boolean o) {
    return (o == null ? fromNull(NullSql.BOOLEAN) : o);
  }

  @Override
  public Object fromString(String o) {
    return (o == null ? fromNull(NullSql.VARCHAR) : o);
  }

  @Override
  public Object fromTiny(Byte o) {
    return (o == null ? fromNull(NullSql.TINY) : o);
  }

  @Override
  public Object fromShort(Short o) {
    return (o == null ? fromNull(NullSql.SMALL) : o);
  }

  @Override
  public Object fromInteger(Integer o) {
    return (o == null ? fromNull(NullSql.INTEGER) : o);
  }

  @Override
  public Object fromLong(Long o) {
    return (o == null ? fromNull(NullSql.BIGINT) : o);
  }

  @Override
  public Object fromDecimal(Double o) {
    return (o == null ? fromNull(NullSql.DECIMAL) : o);
  }

  @Override
  public Object fromBigDecimal(BigDecimal o) {
    return (o == null ? fromNull(NullSql.DECIMAL) : o);
  }

  @Override
  public Object fromTime(final Date o) {
    if (o == null)
      return fromNull(NullSql.TIME);
    else
      return new AbstractPreparedStatementCallback(o) {
        @Override
        public void execute(PreparedStatement ps, int columnIndex) throws SQLException {
          ps.setTime(columnIndex, new Time(o.getTime()));
        }
      };
  }

  @Override
  public Object fromDate(final Date o) {
    if (o == null)
      return fromNull(NullSql.DATE);
    else
      return new AbstractPreparedStatementCallback(o) {
        @Override
        public void execute(PreparedStatement ps, int columnIndex) throws SQLException {
          ps.setDate(columnIndex, new java.sql.Date(o.getTime()));
        }
      };
  }

  @Override
  public Object fromDateTime(final Date o) {
    if (o == null)
      return fromNull(NullSql.TIMESTAMP);
    else
      return new AbstractPreparedStatementCallback(o) {
        @Override
        public void execute(PreparedStatement ps, int columnIndex) throws SQLException {
          ps.setTimestamp(columnIndex, new Timestamp(o.getTime()));
        }
      };
  }

  @Override
  public Object fromTimestamp(final Date o) {
    if (o == null)
      return fromNull(NullSql.TIMESTAMP);
    else
      return new AbstractPreparedStatementCallback(o) {
        @Override
        public void execute(PreparedStatement ps, int columnIndex) throws SQLException {
          // ps.setTimestamp(columnIndex, new java.sql.Timestamp(o.getTime()), Calendar.getInstance(timeZone));
          ps.setTimestamp(columnIndex, new Timestamp(o.getTime()), getCalendar());
        }
      };
  }

  @Override
  public Object fromText(final InputStream is, final int length) {
    if (is == null) {
      return fromNull(NullSql.CLOB);
    } else {
      return new AbstractPreparedStatementCallback(is) {
        @Override
        public void execute(PreparedStatement ps, int columnIndex) throws SQLException {
          ps.setAsciiStream(columnIndex, new AutoCloseInputStream(is), (int) length);
        }
      };
    }
  }

  @Override
  public Object fromBin(final InputStream is, final int length) {
    if (is == null) {
      return fromNull(NullSql.BLOB);
    } else {
      return new AbstractPreparedStatementCallback(is) {
        @Override
        public void execute(PreparedStatement ps, int columnIndex) throws SQLException {
          ps.setBinaryStream(columnIndex, new AutoCloseInputStream(is), length);
        }
      };
    }
  }

  @Override
  public Object fromUnknown(Object o) {
    if (o instanceof Date)
      return fromDate((Date) o);
    if (o instanceof Boolean)
      return fromBoolean((Boolean) o);
    else
      return o;
  }

  @Override
  public boolean ignoreNullKeys() {
    return false;
  }

  @Override
  public String getSql(SqlProcedure procedure) {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    if (procedure.isFunction())
      sb.append(" ? =");
    sb.append(" call ").append(procedureName(procedure)).append("(");

    int start = procedure.isFunction() ? 1 : 0;
    int len = procedure.getParameters().size();
    for (int i = 0; i < len; i++) {
      if (i > start)
        sb.append(", ");
      sb.append("?");
    }

    sb.append(") }");
    return sb.toString();
  }

  public InsertBuilder createInsertBuilder(InsertDSL insert) {
    return new GenericInsertBuilder(insert);
  }

  @Override
  public String getSql(InsertDSL insert) {
    InsertBuilder proc = createInsertBuilder(insert);

    StringBuilder str = new StringBuilder();
    // INSERT
    str.append("INSERT INTO ").append(proc.getTablePart())
        .append("(")
        .append(proc.getColumnPart())
        .append(") VALUES(")
        .append(proc.getValuePart())
        .append(")");

    return str.toString();
  }

  protected String getDefault() {
    return "NULL";
  }

  // UPDATE
  public UpdateBuilder createUpdateBuilder(UpdateDSL update) {
    return new GenericUpdateBuilder(update);
  }

  @Override
  public String getSql(UpdateDSL update) {
    UpdateBuilder proc = createUpdateBuilder(update);

    StringBuilder sel = new StringBuilder();

    // SET
    sel.append("UPDATE ").append(proc.getTablePart());
    sel.append(" SET ").append(proc.getColumnPart());
    // JOINS
    // sel.append(proc.joinPart.String())
    // WHERE - conditions
    if (!proc.getWherePart().isEmpty()) {
      sel.append(" WHERE ").append(proc.getWherePart());
    }

    return sel.toString();
  }

  protected DeleteBuilder createDeleteBuilder(DeleteDSL delete) {
    return new GenericDeleteBuilder(delete);
  }

  // DELETE
  @Override
  public String getSql(DeleteDSL delete) {
    DeleteBuilder processor = createDeleteBuilder(delete);

    StringBuilder sb = new StringBuilder();

    sb.append("DELETE FROM ").append(processor.getTablePart());
    String where = processor.getWherePart();
    if (!where.isEmpty()) {
      sb.append(" WHERE ").append(where);
    }
    return sb.toString();
  }

  @Override
  public String getSql(Sequence sequence, boolean nextValue) {
    throw new UnsupportedOperationException();
  }

  public QueryBuilder createQueryBuilder(QueryDSL query) {
    return new GenericQueryBuilder(query);
  }

  @Override
  public String getSql(QueryDSL query) {
    QueryBuilder proc = this.createQueryBuilder(query);

    // SELECT COLUNAS
    StringBuilder sel = new StringBuilder();
    sel.append("SELECT ");
    if (query.isDistinct()) {
      sel.append("DISTINCT ");
    }
    sel.append(proc.getColumnPart());
    // FROM
    sel.append(" FROM ").append(proc.getFromPart());
    // JOINS
    sel.append(proc.getJoinPart());
    // WHERE - conditions
    if (!proc.getWherePart().isEmpty()) {
      sel.append(" WHERE ").append(proc.getWherePart());
    }
    // GROUP BY
    int[] groupBy = query.getGroupBy();
    if (groupBy != null && groupBy.length != 0) {
      sel.append(" GROUP BY ").append(proc.getGroupPart());
    }
    // HAVING
    if (query.getHaving() != null) {
      sel.append(" HAVING ").append(proc.getHavingPart());
    }
    // UNION
    if (length(query.getUnions()) != 0) {
      sel.append(proc.getUnionPart());
    }
    // ORDER
    if (length(query.getSorts()) != 0) {
      sel.append(" ORDER BY ").append(proc.getOrderPart());
    }

    return paginate(query, sel.toString());
  }

  @Override
  public final void registerQueryMappers(QueryMapper... mappers) {
    registerMappers(this.queryMappers, mappers);
  }

  @Override
  public QueryMapper findQueryMapper(Class<?> klass) {
    return findMapper(queryMappers, klass, QueryMapper.class.getSimpleName());
  }

  @Override
  public final void registerInsertMappers(InsertMapper... mappers) {
    registerMappers(this.insertMappers, mappers);
  }

  @Override
  public InsertMapper findInsertMapper(Class<?> klass) {
    return findMapper(insertMappers, klass, InsertMapper.class.getSimpleName());
  }

  @Override
  public final void registerDeleteMappers(DeleteMapper... mappers) {
    registerMappers(this.deleteMappers, mappers);
  }

  @Override
  public DeleteMapper findDeleteMapper(Class<?> klass) {
    return findMapper(deleteMappers, klass, DeleteMapper.class.getSimpleName());
  }

  @Override
  public final void registerUpdateMappers(UpdateMapper... mappers) {
    registerMappers(this.updateMappers, mappers);
  }

  @Override
  public UpdateMapper findUpdateMapper(Class<?> klass) {
    return findMapper(updateMappers, klass, UpdateMapper.class.getSimpleName());
  }

  @SuppressWarnings("unchecked")
  @SafeVarargs
  private final <T extends MapperSupporter> void registerMappers(Collection<T> mappers, T... newMappers) {
    mappers.clear();
    for (MapperSupporter qm : newMappers) {
      mappers.add((T) qm);
    }
  }

  private <T extends MapperSupporter> T findMapper(Collection<T> supporters, Class<?> klass, String notFoundMsg) {
    return supporters.stream()
        .filter(qm -> qm.support(klass))
        .findFirst()
        .orElseThrow(() ->
            new IllegalArgumentException("Unable to find a " + notFoundMsg + " for " + klass.getCanonicalName())
        );
  }

  @Override
  public Converter getConverter(Class<? extends Converter> converter) {
    return converters.computeIfAbsent(converter, aClass -> {
      try {
        return aClass.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new PersistenceException(e);
      }
    });
  }

  /**
   * This is entry point for resolving functions.<br>
   * For implementing user defined functions this should be overridden.<br>
   * The implementation should also call this super method if the passed function is not resolved by the user implementation.
   *
   * @param function the function to be resolved
   * @return the string representation of the passed function
   */
  @Override
  public String translate(EDml dmlType, Function function) {
    String op = function.getOperator();
    if (EFunction.COLUMN.equals(op)) {
      return columnName(dmlType, function);
    } else if (EFunction.EQ.equals(op)) {

      // CONDITIONS
      return match(dmlType, function);
    } else if (EFunction.NEQ.equals(op)) {
      return diferent(dmlType, function);
    } else if (EFunction.IN.equals(op)) {
      return in(dmlType, function);
    } else if (EFunction.RANGE.equals(op)) {
      return range(dmlType, function);
    } else if (EFunction.VALUERANGE.equals(op)) {
      return valueRange(dmlType, function);
    } else if (EFunction.BOUNDEDRANGE.equals(op)) {
      return boundedValueRange(dmlType, function);
    } else if (EFunction.ISNULL.equals(op)) {
      return isNull(dmlType, function);
    } else if (EFunction.LIKE.equals(op)) {
      return like(dmlType, function);
    } else if (EFunction.ILIKE.equals(op)) {
      return ilike(dmlType, function);
    } else if (EFunction.IEQ.equals(op)) {
      return iMatch(dmlType, function);
    } else if (EFunction.AND.equals(op)) {
      return and(dmlType, function);
    } else if (EFunction.OR.equals(op)) {
      return or(dmlType, function);
    } else if (EFunction.GT.equals(op)) {
      return greater(dmlType, function);
    } else if (EFunction.LT.equals(op)) {
      return lesser(dmlType, function);
    } else if (EFunction.GTEQ.equals(op)) {
      return greaterOrMatch(dmlType, function);
    } else if (EFunction.LTEQ.equals(op)) {
      return lesserOrMatch(dmlType, function);
    } else if (EFunction.EXISTS.equals(op)) {
      return exists(dmlType, function);
			/*
		} else if (EFunction.NOT.equals(op)) {
			return not(dmlType, function);
    */
      // FUNCTIONS
    } else if (EFunction.PARAM.equals(op)) {
      return param(dmlType, function);
    } else if (EFunction.RAW.equals(op) || EFunction.ASIS.equals(op)) {
      return val(dmlType, function);
    } else if (EFunction.ALIAS.equals(op)) {
      return alias(dmlType, function);
    } else if (EFunction.COUNT.equals(op)) {
      return count(dmlType, function);
    } else if (EFunction.ADD.equals(op)) {
      return add(dmlType, function);
    } else if (EFunction.MINUS.equals(op)) {
      return minus(dmlType, function);
    } else if (EFunction.SECONDSDIFF.equals(op)) {
      return secondsdiff(dmlType, function);
    } else if (EFunction.SUM.equals(op)) {
      return sum(dmlType, function);
    } else if (EFunction.MAX.equals(op)) {
      return max(dmlType, function);
    } else if (EFunction.MIN.equals(op)) {
      return min(dmlType, function);
    } else if (EFunction.MULTIPLY.equals(op)) {
      return multiply(dmlType, function);
    } else if (EFunction.RTRIM.equals(op)) {
      return rtrim(dmlType, function);
    } else if (EFunction.NOW.equals(op)) {
      return now(dmlType, function);
    } else if (EFunction.SUBQUERY.equals(op)) {
      return subQuery(dmlType, function);
    } else if (EFunction.AUTONUM.equals(op)) {
      return autoNumber(dmlType, function);
    } else if (EFunction.UPPER.equals(op)) {
      return upper(dmlType, function);
    } else if (EFunction.LOWER.equals(op)) {
      return lower(dmlType, function);
    } else if (EFunction.COALESCE.equals(op)) {
      return coalesce(dmlType, function);
    } else if (EFunction.CASE.equals(op)) {
      return caseStatement(dmlType, function);
    } else if (EFunction.CASE_WHEN.equals(op)) {
      return caseWhen(dmlType, function);
    } else if (EFunction.CASE_ELSE.equals(op)) {
      return caseElse(dmlType, function);
    } else
      throw new PersistenceException("Function " + op + " unknown");
  }

  protected String rolloverParameter(EDml dmlType, Object[] o, String separator) {
    StringBuilder sb = new StringBuilder();
    for (int f = 0; f < o.length; f++) {
      if (f > 0 && separator != null)
        sb.append(separator);
      sb.append(translate(dmlType, (Function) o[f]));
    }
    return sb.toString();
  }

  private String isNot(Condition c) {
    return c.isNot() ? "NOT " : "";
  }

  // TO BE OVERRIDEN ======================

  @Override
  public boolean useSQLPagination() {
    return true;
  }

  @Override
  public String tableName(Table table) {
    return table.getName();
  }

  @Override
  public String tableAlias(String alias) {
    return alias;
  }

  @Override
  public String columnName(Column<?> column) {
    return column.getName();
  }

  @Override
  public String procedureName(SqlProcedure procedure) {
    return procedure.getName();
  }

  @Override
  public String columnAlias(Function function, int position) {
    String tableAlias = function.getPseudoTableAlias();
    if (tableAlias != null) {
      return tableAlias + "_" + function.getAlias();
    } else {
      return function.getAlias();
    }
  }

  public String columnName(EDml dmlType, Function function) {
    if (function instanceof ColumnHolder) {
      ColumnHolder ch = (ColumnHolder) function;
      return (ch.getTableAlias() == null ? tableName(ch.getColumn().getTable()) : ch.getTableAlias()) + "." + columnName(ch.getColumn());
    } else
      return "";
  }

  public String match(EDml dmlType, Function function) {
    Object[] o = function.getMembers();
    return String.format("%s = %s", translate(dmlType, (Function) o[0]), translate(dmlType, (Function) o[1]));
  }

  public String iMatch(EDml dmlType, Function function) {
    Object[] o = function.getMembers();
    return String.format("UPPER(%s) = UPPER(%s)", translate(dmlType, (Function) o[0]), translate(dmlType, (Function) o[1]));
  }

  public String diferent(EDml dmlType, Function function) {
    Object[] o = function.getMembers();
    return String.format("%s <> %s", translate(dmlType, (Function) o[0]), translate(dmlType, (Function) o[1]));
  }

  public String range(EDml dmlType, Function function) {
    Object[] o = function.getMembers();
    String field = translate(dmlType, (Function) o[0]);
    String bottom = translate(dmlType, (Function) o[1]);
    String top = translate(dmlType, (Function) o[2]);
    if (bottom != null && top != null)
      return String.format("%s >= %s AND %s <= %s", field, bottom, field, top);
    else
      throw new PersistenceException("Função Range Invalida");
  }

  public String valueRange(EDml dmlType, Function function) {
    Object[] o = function.getMembers();
    String bottom = translate(dmlType, (Function) o[0]);
    String top = translate(dmlType, (Function) o[1]);
    String value = null;
    if (o[2] != null)
      value = translate(dmlType, (Function) o[2]);

    if (value != null)
      return String.format("(%1$s IS NULL AND %2$s IS NULL OR %1$s IS NULL AND %2$s <= %3$s OR %2$s IS NULL AND %1$s >= %3$s OR %1$s >= %3$s AND %2$s <= %3$s)",
          top, bottom, value);
    else
      throw new PersistenceException("Invalid ValueRange Function");
  }

  public String boundedValueRange(EDml dmlType, Function function) {
    Object[] o = function.getMembers();
    String bottom = translate(dmlType, (Function) o[0]);
    String top = translate(dmlType, (Function) o[1]);
    String value = null;
    if (o[2] != null)
      value = translate(dmlType, (Function) o[2]);

    if (value != null)
      return String.format("(%1$s >= %3$s AND %2$s <= %3$s)", top, bottom, value);
    else
      throw new PersistenceException("Invalid BoundedRange Function");
  }

  public String in(EDml dmlType, Function function) {
    Condition c = (Condition) function;
    Object[] o = function.getMembers();
    String pattern = null;
    if (((Function) o[1]).getOperator().equals(EFunction.SUBQUERY))
      pattern = "%s%s IN %s";
    else
      pattern = "%s%s IN (%s)";

    return String.format(pattern, isNot(c), translate(dmlType, (Function) o[0]), rolloverParameter(dmlType, slice(o, 1), ", "));
  }

  public String or(EDml dmlType, Function function) {
    Object[] o = function.getMembers();
    return String.format("(%s)", rolloverParameter(dmlType, o, " OR "));
  }

  public String and(EDml dmlType, Function function) {
    Object[] o = function.getMembers();
    return String.format("%s", rolloverParameter(dmlType, o, " AND "));
  }

  public String like(EDml dmlType, Function function) {
    Condition c = (Condition) function;
    Object[] o = function.getMembers();
    return String.format("%s %sLIKE %s",
        translate(dmlType, (Function) o[0]), isNot(c), translate(dmlType, (Function) o[1]));
  }

  public String ilike(EDml dmlType, Function function) {
    Condition c = (Condition) function;
    Object[] o = function.getMembers();
    return String.format("UPPER(%s) %sLIKE UPPER(%s)",
        translate(dmlType, (Function) o[0]), isNot(c), translate(dmlType, (Function) o[1]));
  }

  public String isNull(EDml dmlType, Function function) {
    Condition c = (Condition) function;
    Object[] o = function.getMembers();
    return String.format("%s IS %sNULL", translate(dmlType, (Function) o[0]), isNot(c));
  }

  public String greater(EDml dmlType, Function function) {
    Object[] o = function.getMembers();
    return String.format("%s > %s", translate(dmlType, (Function) o[0]), translate(dmlType, (Function) o[1]));
  }

  public String lesser(EDml dmlType, Function function) {
    Object[] o = function.getMembers();
    return String.format("%s < %s", translate(dmlType, (Function) o[0]), translate(dmlType, (Function) o[1]));
  }

  public String greaterOrMatch(EDml dmlType, Function function) {
    Object[] o = function.getMembers();
    return String.format("%s >= %s", translate(dmlType, (Function) o[0]), translate(dmlType, (Function) o[1]));
  }

  public String lesserOrMatch(EDml dmlType, Function function) {
    Object[] o = function.getMembers();
    return String.format("%s <= %s", translate(dmlType, (Function) o[0]), translate(dmlType, (Function) o[1]));
  }

  // FUNCTIONS
  private Object[] slice(Object o[], int begin) {
    Object[] pars = new Object[o.length - begin];
    System.arraycopy(o, begin, pars, 0, pars.length);
    return pars;
  }

  public String param(EDml dmlType, Function function) {
    return ":" + function.getValue();
  }

  public String val(EDml dmlType, Function function) {
    Object o = function.getValue();
    return (o != null ? (o instanceof String ? "'" + o + "'" : o.toString()) : "NULL");
  }

  public String exists(EDml dmlType, Function function) {
    Condition c = (Condition) function;
    Object[] o = function.getMembers();
    return String.format("%sEXISTS %s", isNot(c), translate(dmlType, (Function) o[0]));
  }

	/*
	public String not(EDml dmlType, Function function) {
		Object[] o = function.getMembers();
		return String.format("NOT %s", translate(dmlType, (Function) o[0]));
	}
	*/

  public String alias(EDml dmlType, Function function) {
    Object o = function.getValue();
    return (o != null ? o.toString() : "NULL");
  }

  public String sum(EDml dmlType, Function function) {
    Object[] o = function.getMembers();
    return String.format("SUM(%s)", rolloverParameter(dmlType, o, ", "));
  }

  public String max(EDml dmlType, Function function) {
    Object[] o = function.getMembers();
    return String.format("MAX(%s)", rolloverParameter(dmlType, o, ", "));
  }

  public String min(EDml dmlType, Function function) {
    Object[] o = function.getMembers();
    return String.format("MIN(%s)", rolloverParameter(dmlType, o, ", "));
  }

  public String add(EDml dmlType, Function function) {
    Object[] o = function.getMembers();
    return rolloverParameter(dmlType, o, " + ");
  }

  public String minus(EDml dmlType, Function function) {
    Object[] o = function.getMembers();
    return rolloverParameter(dmlType, o, " - ");
  }

  public String secondsdiff(EDml dmlType, Function function) {
    Object[] o = function.getMembers();
    // swap
    return rolloverParameter(dmlType, new Object[]{o[1], o[0]}, " - ");
  }

  public String multiply(EDml dmlType, Function function) {
    Object[] o = function.getMembers();
    return rolloverParameter(dmlType, o, " * ");
  }

  public String count(EDml dmlType, Function function) {
    Object[] o = function.getMembers();
    return String.format("COUNT(%s)", o.length == 0 ? "*" : translate(dmlType, (Function) o[0]));
  }

  public String rtrim(EDml dmlType, Function function) {
    Object[] o = function.getMembers();
    return String.format("RTRIM(%s)", translate(dmlType, (Function) o[0]));
  }

  public String subQuery(EDml dmlType, Function function) {
    return String.format("( %s )", getSql((QueryDSL) function.getValue()));
  }

  public String now(EDml dmlType, Function function) {
    throw new UnsupportedOperationException("O metodo 'now' não é suportado.");
  }

  public String upper(EDml dmlType, Function function) {
    Object[] o = function.getMembers();
    return String.format("UPPER(%s)", translate(dmlType, (Function) o[0]));
  }

  public String lower(EDml dmlType, Function function) {
    Object[] o = function.getMembers();
    return String.format("LOWER(%s)", translate(dmlType, (Function) o[0]));
  }

  public String coalesce(EDml dmlType, Function function) {
    Object[] o = function.getMembers();
    return String.format("COALESCE(%s)", rolloverParameter(dmlType, o, ", "));
  }

  public String caseStatement(EDml dmlType, Function function) {
    Object[] o = function.getMembers();
    return String.format("CASE %s END", rolloverParameter(dmlType, o, " "));
  }

  public String caseWhen(EDml dmlType, Function function) {
    Object[] o = function.getMembers();
    return String.format("WHEN %s THEN %s", translate(dmlType, (Function) o[0]), translate(dmlType, (Function) o[1]));
  }

  public String caseElse(EDml dmlType, Function function) {
    Object[] o = function.getMembers();
    return String.format("ELSE %s", translate(dmlType, (Function) o[0]));
  }

  public String autoNumber(EDml dmlType, Function function) {
    throw new UnsupportedOperationException();
  }

  // ================================

  @Override
  public int paginationColumnOffset(QueryDSL query) {
    return 0;
  }

  @Override
  public void prepareConnection(Connection connection) {

  }

  public abstract String paginate(QueryDSL query, String sql);

  protected Object toDefault(ResultSetWrapper rsw, int position) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    Object o = rs.getObject(position);
    return rs.wasNull() ? null : o;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T fromDb(ResultSetWrapper rsw, int position, Class<T> klass) throws SQLException {
    if (klass == null) {
      return (T) toDefault(rsw, position);
    } else {

      Object val = null;
      if (klass.isAssignableFrom(Boolean.class) || klass.isAssignableFrom(boolean.class))
        val = toBoolean(rsw, position);
      else if (klass.isAssignableFrom(BigDecimal.class))
        val = toBigDecimal(rsw, position);
      else if (klass.isAssignableFrom(Byte.class) || klass.isAssignableFrom(byte.class))
        val = toTiny(rsw, position);
      else if (klass.isAssignableFrom(Short.class) || klass.isAssignableFrom(short.class))
        val = toShort(rsw, position);
      else if (klass.isAssignableFrom(Integer.class) || klass.isAssignableFrom(int.class))
        val = toInteger(rsw, position);
      else if (klass.isAssignableFrom(Long.class) || klass.isAssignableFrom(long.class))
        val = toLong(rsw, position);
      else if (klass.isAssignableFrom(Double.class) || klass.isAssignableFrom(double.class))
        val = toDecimal(rsw, position);
      else if (klass.isAssignableFrom(Date.class))
        val = toTimestamp(rsw, position);
      else if (klass.isAssignableFrom(MyTime.class))
        val = toTime(rsw, position);
      else if (klass.isAssignableFrom(MyDate.class))
        val = toDate(rsw, position);
      else if (klass.isAssignableFrom(MyDateTime.class))
        val = toDateTime(rsw, position);
      else if (TextStore.class.isAssignableFrom(klass)) {
        val = new TextStore();
        Misc.copy(toText(rsw, position), (BinStore) val);
      } else if (BinStore.class.isAssignableFrom(klass)) {
        val = new BinStore();
        Misc.copy(toBin(rsw, position), (BinStore) val);
      } else if (klass.isEnum()) {
        int sqlType = rsw.getSqlType(position);
        if (Types.VARCHAR == sqlType) {
          String value = toString(rsw, position);
          if (value != null) {
            if (Value.class.isAssignableFrom(klass)) {
              for (final Object element : klass.getEnumConstants()) {
                Value<?> e = (Value<?>) element;
                if (e.value().equals(value)) {
                  val = element;
                  break;
                }
              }
            } else {
              for (final Object element : klass.getEnumConstants()) {
                Enum<?> e = (Enum<?>) element;
                if (e.name().equals(value)) {
                  val = element;
                  break;
                }
              }
            }
          }
        } else {
          Integer value = toInteger(rsw, position);
          if (value != null) {
            int v = value.intValue();
            if (Value.class.isAssignableFrom(klass)) {
              for (final Object element : klass.getEnumConstants()) {
                Value<?> e = (Value<?>) element;
                if (((Number) e.value()).intValue() == v) {
                  val = element;
                  break;
                }
              }
            } else {
              for (final Object element : klass.getEnumConstants()) {
                Enum<?> e = (Enum<?>) element;
                if (e.ordinal() == v) {
                  val = element;
                  break;
                }
              }
            }
          }
        }
      } else if (klass.isAssignableFrom(byte[].class)) {
        // Types.BLOB == sqlType || Types.LONGVARBINARY == sqlType
        // since will be setting all bytes, ByteCahce is not necessary
        InputStream in = toBin(rsw, position);
        if (in != null) {
          try {
            val = IOUtils.toByteArray(in);
          } catch (IOException e) {
            throw new PersistenceException("Unable to convert stream into bytes!", e);
          } finally {
            IOUtils.closeQuietly(in);
          }
        }
      } else if (klass.isAssignableFrom(String.class)) {
        int sqlType = rsw.getSqlType(position);
        if (Types.CLOB == sqlType || Types.LONGNVARCHAR == sqlType) {
          // since will be setting all bytes, TextCache is not necessary
          InputStream in = toText(rsw, position);
          if (in != null) {
            try {
              val = IOUtils.toString(in, TextStore.DEFAULT_CHARSET);
            } catch (IOException e) {
              throw new PersistenceException("Unable to convert stream into bytes!", e);
            } finally {
              IOUtils.closeQuietly(in);
            }
          }
        } else
          val = toString(rsw, position);
      }

      return (T) val;
    }
  }
}