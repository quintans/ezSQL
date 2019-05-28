package com.github.quintans.ezSQL.dml;

import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.jdbc.ColumnType;
import com.github.quintans.jdbc.RawSql;
import com.github.quintans.jdbc.SimpleJdbc;
import com.github.quintans.jdbc.transformers.IResultTransformer;
import org.apache.log4j.Logger;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class JdbcExecutor {
  private static Logger LOGGER = Logger.getLogger(JdbcExecutor.class);

  private Driver driver;
  private SimpleJdbc simpleJdbc;

  private int batchLimit = 1000;
  private RawSql lastRawSql;

  public JdbcExecutor(Driver driver, SimpleJdbc simpleJdbc) {
    this.driver = driver;
    this.simpleJdbc = simpleJdbc;
  }

  public <T> T queryUnique(String sql, IResultTransformer<T> rt, Map<String, Object> parameters) {
    RawSql rawSql = parseSql(sql);
    Map<String, Object> params = transformParameters(parameters);
    return simpleJdbc.queryUnique(rawSql.getJdbcSql(), rt, rawSql.buildValues(params));
  }

  public <T> List<T> queryRange(String sql, IResultTransformer<T> rt, int firstRow, int maxRows, Map<String, Object> parameters) {
    RawSql rawSql = parseSql(sql);
    Map<String, Object> params = transformParameters(parameters);
    return simpleJdbc.queryRange(rawSql.getJdbcSql(), rt, firstRow, maxRows, rawSql.buildValues(params));
  }

  public int execute(String sql, Map<String, Object> parameters) {
    // just in case;
    simpleJdbc.closeBatch();
    debugSQL(sql, parameters);

    Map<String, Object> pars = transformParameters(parameters);

    RawSql rawSql = parseSql(sql);
    int i = simpleJdbc.update(rawSql.getJdbcSql(), rawSql.buildValues(pars));
    debug("result = %s", i);
    return i;
  }

  private Map<String, Object> transformParameters(Map<String, Object> parameters) {
    Map<String, Object> pars = new LinkedHashMap<String, Object>();
    for (Map.Entry<String, Object> entry : parameters.entrySet()) {
      Object val = entry.getValue();
      val = driver.transformParameter(val);
      pars.put(entry.getKey(), val);
    }

    return pars;
  }

  public Object[] transformParameters(Object... parameters) {
    if (parameters == null)
      return null;

    Object[] vals = new Object[parameters.length];
    int i = 0;
    for (Object parameter : parameters) {
      vals[i++] = driver.transformParameter(parameter);
    }

    return vals;
  }

  public int[] batch(String sql, Map<String, Object> parameters) {
    Map<String, Object> pars = transformParameters(parameters);
    RawSql rawSql = parseSql(sql);
    simpleJdbc.batch(rawSql.getJdbcSql(), rawSql.buildValues(pars));
    if (simpleJdbc.getPending() >= batchLimit) {
      return flushBatch(sql, parameters);
    }
    return null;
  }

  public int[] flushBatch(String sql, Map<String, Object> parameters) {
    int[] result = null;
    if (simpleJdbc.getPending() > 0) {
      RawSql rawSql = parseSql(sql);
      debugSQL(rawSql.getOriginalSql(), parameters);
      result = simpleJdbc.flushUpdate();
    }
    return result;
  }

  public void endBatch(String sql, Map<String, Object> parameters) {
    flushBatch(sql, parameters);
    simpleJdbc.closeBatch();
  }

  /**
   * batch statements that are still pending to execute
   *
   * @return
   */
  public int getPending() {
    return simpleJdbc.getPending();
  }

  public int getBatchLimit() {
    return batchLimit;
  }

  /**
   * Defines the amount of pending DML commands at witch the ezSQL will flush them to the Database.
   *
   * @param batchLimit
   */
  public void batchLimit(int batchLimit) {
    this.batchLimit = batchLimit;
  }

  public Long fetchAutoNumberBefore(Column<? extends Number> column) {
    return fetchAutoNumber(column, false);
  }

  public Long fetchCurrentAutoNumberAfter(Column<? extends Number> column) {
    return fetchAutoNumber(column, true);
  }

  public Long fetchAutoNumber(Column<? extends Number> column, boolean after) {
    String sql = after ? this.driver.getCurrentAutoNumberQuery(column) : this.driver.getAutoNumberQuery(column);
    long now = 0;
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("SQL: " + sql);
      now = System.nanoTime();
    }
    Long id = simpleJdbc.queryForLong(sql, new LinkedHashMap<>());
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("executed in: " + (System.nanoTime() - now) / 1e6 + "ms");
    }
    return id;
  }

  public int update(String sql, Map<String, Object> parameters) {
    RawSql rawSql = parseSql(sql);
    Map<String, Object> params = transformParameters(parameters);
    return simpleJdbc.update(rawSql.getJdbcSql(), rawSql.buildValues(params));
  }

  public Object[] insert(String sql, ColumnType[] keyColumnTypes, Map<String, Object> parameters) {
    RawSql rawSql = parseSql(sql);
    Map<String, Object> params = transformParameters(parameters);
    return simpleJdbc.insert(rawSql.getJdbcSql(), keyColumnTypes, rawSql.buildValues(params));
  }

  private RawSql parseSql(String sql) {
    if(lastRawSql == null || !Objects.equals(lastRawSql.getOriginalSql(), sql)) {
      lastRawSql = RawSql.of(sql);
    }
    return lastRawSql;
  }

  private void debug(String format, Object... args) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(String.format(format, args));
    }
  }

  private void debugSQL(String sql, Map<String, Object> parameters) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          String.format("SQL: %s\n\tparameters: %s", sql, dumpParameters(parameters)));
    }
  }

  private String dumpParameters(Map<String, Object> map) {
    StringBuilder sb = new StringBuilder();

    for (Map.Entry<String, Object> entry : map.entrySet()) {
      if (entry.getKey().endsWith("$")) {
        // secret
        sb.append(String.format("[%s=****]", entry.getKey()));
      } else {
        sb.append(String.format("[%s=%s]", entry.getKey(), entry.getValue()));
      }
    }

    return sb.toString();
  }
}
