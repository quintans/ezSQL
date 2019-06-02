package com.github.quintans.ezSQL.translator;

import com.github.quintans.ezSQL.common.api.Converter;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.SequenceDSL;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.dml.DeleteDSL;
import com.github.quintans.ezSQL.dml.Function;
import com.github.quintans.ezSQL.dml.InsertDSL;
import com.github.quintans.ezSQL.dml.QueryDSL;
import com.github.quintans.ezSQL.dml.UpdateDSL;
import com.github.quintans.ezSQL.mapper.DeleteMapper;
import com.github.quintans.ezSQL.mapper.InsertMapper;
import com.github.quintans.ezSQL.mapper.QueryMapper;
import com.github.quintans.ezSQL.mapper.UpdateMapper;
import com.github.quintans.ezSQL.sp.SqlProcedureDSL;

public interface Translator {

  String translate(EDml dmlType, Function function);

  boolean ignoreNullKeys();

  String getSql(SqlProcedureDSL procedure);

  String getSql(InsertDSL insert);

  String getSql(QueryDSL query);

  String getSql(UpdateDSL update);

  String getSql(DeleteDSL delete);

  String getSql(SequenceDSL sequence, boolean nextValue);

  String getAutoNumberQuery(Column<? extends Number> column);

  String getCurrentAutoNumberQuery(Column<? extends Number> column);

  boolean useSQLPagination();

  int paginationColumnOffset(QueryDSL query);

  String tableName(Table table);

  String tableAlias(String alias);

  String columnName(Column<?> column);

  String procedureName(SqlProcedureDSL procedure);

  String columnAlias(Function function, int position);

  void registerQueryMappers(QueryMapper... mappers);

  QueryMapper findQueryMapper(Class<?> klass);

  void registerInsertMappers(InsertMapper... mappers);

  InsertMapper findInsertMapper(Class<?> klass);

  void registerDeleteMappers(DeleteMapper... mappers);

  DeleteMapper findDeleteMapper(Class<?> klass);

  void registerUpdateMappers(UpdateMapper... mappers);

  UpdateMapper findUpdateMapper(Class<?> klass);

  Converter getConverter(Class<? extends Converter> converter);

}
