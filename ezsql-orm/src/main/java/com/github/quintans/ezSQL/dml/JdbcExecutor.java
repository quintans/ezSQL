package com.github.quintans.ezSQL.dml;

import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.jdbc.ColumnType;
import com.github.quintans.jdbc.RawSql;
import com.github.quintans.jdbc.SimpleJdbc;
import com.github.quintans.jdbc.transformers.IRowTransformer;
import org.apache.log4j.Logger;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class JdbcExecutor {
  private static Logger LOGGER = Logger.getLogger(JdbcExecutor.class);

  private Driver driver;
  private SimpleJdbc simpleJdbc;

  private int batchLimit = 1000;

  public JdbcExecutor(Driver driver, SimpleJdbc simpleJdbc) {
    this.driver = driver;
    this.simpleJdbc = simpleJdbc;
  }

  public <T> T queryUnique(String sql, IRowTransformer<T> rt, Object... params) {
    return simpleJdbc.queryUnique(sql, rt, params);
  }

  public <T> List<T> queryRange(String sql, IRowTransformer<T> rt, int firstRow, int maxRows, Object... params) {
    return simpleJdbc.queryRange(sql, rt, firstRow, maxRows, params);
  }

  public int execute(RawSql rawSql, Map<String, Object> parameters) {
    // just in case;
    simpleJdbc.closeBatch();
    debugSQL(rawSql.getOriginalSql(), parameters);

    Map<String, Object> pars = transformParameters(parameters);

    int i = simpleJdbc.update(rawSql.getSql(), rawSql.buildValues(pars));
    debug("result = %s", i);
    return i;
  }

  public Map<String, Object> transformParameters(Map<String, Object> parameters) {
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

  public int[] batch(RawSql rawSql, Map<String, Object> parameters) {
    Map<String, Object> pars = transformParameters(parameters);
    simpleJdbc.batch(rawSql.getSql(), rawSql.buildValues(pars));
    if (simpleJdbc.getPending() >= batchLimit) {
      return flushBatch(rawSql, parameters);
    }
    return null;
  }

  public int[] flushBatch(RawSql rawSql, Map<String, Object> parameters) {
    int[] result = null;
    if (simpleJdbc.getPending() > 0) {
      debugSQL(rawSql.getOriginalSql(), parameters);
      result = simpleJdbc.flushUpdate();
    }
    return result;
  }

  public void endBatch(RawSql rawSql, Map<String, Object> parameters) {
    flushBatch(rawSql, parameters);
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
    Long id = simpleJdbc.queryForLong(sql, new LinkedHashMap<String, Object>());
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("executed in: " + (System.nanoTime() - now) / 1e6 + "ms");
    }
    return id;
  }

  public int update(String sql, Map<String, Object> params) {
    return simpleJdbc.update(sql, params);
  }

  public int update(String sql, Object... params) {
    return simpleJdbc.update(sql, params);
  }

  public Object[] insert(String sql, ColumnType[] keyColumnTypes, Object... params) {
    return simpleJdbc.insert(sql, keyColumnTypes, params);
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
