package com.github.quintans.ezSQL.translator;

import com.github.quintans.ezSQL.common.api.Converter;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.SequenceDSL;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.dml.ColumnHolder;
import com.github.quintans.ezSQL.dml.Condition;
import com.github.quintans.ezSQL.dml.DeleteDSL;
import com.github.quintans.ezSQL.dml.EFunction;
import com.github.quintans.ezSQL.dml.Function;
import com.github.quintans.ezSQL.dml.InsertDSL;
import com.github.quintans.ezSQL.dml.QueryDSL;
import com.github.quintans.ezSQL.dml.UpdateDSL;
import com.github.quintans.ezSQL.exception.OrmException;
import com.github.quintans.ezSQL.mapper.DeleteMapper;
import com.github.quintans.ezSQL.mapper.DeleteMapperBean;
import com.github.quintans.ezSQL.mapper.InsertMapper;
import com.github.quintans.ezSQL.mapper.InsertMapperBean;
import com.github.quintans.ezSQL.mapper.MapperSupporter;
import com.github.quintans.ezSQL.mapper.QueryMapper;
import com.github.quintans.ezSQL.mapper.QueryMapperBean;
import com.github.quintans.ezSQL.mapper.UpdateMapper;
import com.github.quintans.ezSQL.mapper.UpdateMapperBean;
import com.github.quintans.ezSQL.sp.SqlProcedureDSL;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import static com.github.quintans.ezSQL.toolkit.utils.Misc.length;

public abstract class GenericTranslator implements Translator {
  private Collection<QueryMapper> queryMappers;
  private Collection<InsertMapper> insertMappers;
  private Collection<DeleteMapper> deleteMappers;
  private Collection<UpdateMapper> updateMappers;
  private ConcurrentHashMap<Class<? extends Converter>, Converter> converters;

  public GenericTranslator() {
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
  public boolean ignoreNullKeys() {
    return false;
  }

  @Override
  public String getSql(SqlProcedureDSL procedure) {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    if (procedure.isFunction())
      sb.append(" ? =");
    sb.append(" call ").append(procedureName(procedure)).append("(");

    int start = procedure.isFunction() ? 1 : 0;
    int len = procedure.getParametersSize();
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
  public String getSql(SequenceDSL sequence, boolean nextValue) {
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
        throw new OrmException(e);
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
      throw new OrmException("Function " + op + " unknown");
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
  public String procedureName(SqlProcedureDSL procedure) {
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
      throw new OrmException("Invalid Range Function");
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
      throw new OrmException("Invalid ValueRange Function");
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
      throw new OrmException("Invalid BoundedRange Function");
  }

  public String in(EDml dmlType, Function function) {
    Condition c = (Condition) function;
    Object[] o = function.getMembers();
    String pattern;
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
  private Object[] slice(Object[] o, int begin) {
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

  public abstract String paginate(QueryDSL query, String sql);
}