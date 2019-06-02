package com.github.quintans.ezSQL.dml;

import com.github.quintans.ezSQL.AbstractDb;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.mapper.QueryMapper;
import com.github.quintans.ezSQL.transformers.AbstractResultTransformer;
import com.github.quintans.ezSQL.transformers.IRecordTransformer;
import com.github.quintans.ezSQL.transformers.MapTransformer;
import com.github.quintans.ezSQL.transformers.Record;
import com.github.quintans.jdbc.SimpleJdbc;
import com.github.quintans.jdbc.exceptions.PersistenceException;
import com.github.quintans.jdbc.transformers.IResultTransformer;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;
import com.github.quintans.jdbc.transformers.SimpleAbstractResultTransformer;

import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;

import static com.github.quintans.ezSQL.toolkit.utils.Misc.length;

public class Query extends QueryDSL<Query> {

  private AbstractDb db;
  private JdbcExecutor executor;

  public Query(AbstractDb db, Table table) {
    super(db.getTranslator(), table);
    this.db = db;
    this.executor = new JdbcExecutor(db.getTranslator(), db.getDriver(), new SimpleJdbc(db.getJdbcSession()));
  }

  public Query(Query subquery) {
    super(subquery.db.getTranslator(), null);
    this.db = subquery.db;
    this.executor = subquery.executor;
  }

  public Driver getDriver() {
    return db.getDriver();
  }

  public AbstractDb getDb() {
    return db;
  }

  public void copy(Query other) {
    super.copy(other);
    this.db = other.db;
    this.executor = other.executor;
  }

  // ======== RETRIVE ==============

  private Record newRecord(ResultSetWrapper rsw) {
    return getDriver().newRecord(this, rsw);
  }

  public void run(final Consumer<Record> processor) {
    list(createTransformer(processor));
  }

  public void runOne(final Consumer<Record> processor) {
    fetchUnique(createTransformer(processor));
  }

  public void runFirst(final Consumer<Record> processor) {
    select(createTransformer(processor));
  }

  private AbstractResultTransformer<Void> createTransformer(final Consumer<Record> processor) {
    return new AbstractResultTransformer<Void>() {
      @Override
      public Void transform(ResultSetWrapper rsw) throws SQLException {
        processor.accept(newRecord(rsw));
        return null;
      }
    };
  }

  public List<Object[]> listRaw(final Class<?>... clazzes) {
    return list(createRawTransformer(clazzes));
  }

  public Object[] single(final Class<?>... clazzes) {
    return fetchUnique(createRawTransformer(clazzes));
  }

  private SimpleAbstractResultTransformer<Object[]> createRawTransformer(final Class<?>... clazzes) {
    if (length(clazzes) == 0) {
      throw new PersistenceException("Classes must be defined!");
    }

    return new SimpleAbstractResultTransformer<Object[]>() {
      @Override
      public Object[] transform(ResultSetWrapper rsw) throws SQLException {
        return Query.this.transform(rsw, clazzes);
      }
    };
  }

  private Object[] transform(ResultSetWrapper rsw, Class<?>... clazzes) throws SQLException {
    Record record = newRecord(rsw);
    int[] columnTypes = rsw.getColumnTypes();
    int cnt;
    if (clazzes.length > 0)
      cnt = Math.min(columnTypes.length, clazzes.length);
    else
      cnt = columnTypes.length;
    Object[] objs = new Object[cnt];
    for (int i = 0; i < cnt; i++) {
      objs[i] = record.get(i + 1, clazzes[i]);
    }
    return objs;
  }


  /**
   * Retrieves a collection of objects of simple type (not beans). Ex: Boolean,
   * String, enum, ...
   *
   * @param clazz class of the object to return
   * @param <T>   generic
   * @return list of raw elements
   */
  public <T> List<T> listRaw(final Class<T> clazz) {
    final int offset = paginationColumnOffset();

    return list(new SimpleAbstractResultTransformer<T>() {
      @Override
      public T transform(ResultSetWrapper rsw) throws SQLException {
        return newRecord(rsw).get(1, clazz);
      }
    });
  }

  public <T> T single(final Class<T> clazz) {
    return fetchUnique(new SimpleAbstractResultTransformer<T>() {
      @Override
      public T transform(ResultSetWrapper rsw) throws SQLException {
        return newRecord(rsw).get(1, clazz);
      }
    });
  }

  private <T> T fetchUnique(IResultTransformer<T> rt) {
    final int offset = paginationColumnOffset();
    return executor.queryUnique(getSql(), rt, this.parameters);
  }

  /**
   * Executes a query and transform the results to the bean type,<br>
   * matching the alias with bean property name.
   *
   * @param <T>   the bean type
   * @param klass The bean type
   * @param reuse Indicates if for the same entity, a new bean should be
   *              created, or reused a previous instanciated one.
   * @return A collection of beans
   */
  public <T> List<T> list(Class<T> klass, boolean reuse) {
    return list(new MapTransformer<>(this, reuse, klass, getTranslator().findQueryMapper(klass)));
  }


  /**
   * Executes a query and transform the results to the bean type passed as
   * parameter,<br>
   * matching the alias with bean property name. If no alias is supplied, it
   * is used the column alias.
   *
   * @param klass class of the object to return
   * @param <T>   type of klass
   * @return list of type T
   */
  public <T> List<T> list(Class<T> klass) {
    return list(klass, true);
  }

  private <T> IResultTransformer<T> toRowTransformer(final IRecordTransformer<T> recordTransformer) {
    return new SimpleAbstractResultTransformer<T>() {
      @Override
      public T transform(ResultSetWrapper rsw) throws SQLException {
        return recordTransformer.transform(db.getDriver().newRecord(Query.this, rsw));
      }
    };
  }

  /**
   * Executes a query and transforms the row with a record transformer
   *
   * @param recordTransformer record Transformer
   * @param <T>               type of beans to transform to
   * @return list of beans
   */
  public <T> List<T> list(final IRecordTransformer<T> recordTransformer) {
    return list(toRowTransformer(recordTransformer));
  }

  /**
   * Executes a query and transform the results according to the transformer
   *
   * @param <T>       the bean type
   * @param rowMapper The row transformer
   * @return A collection of transformed results
   */
  public <T> List<T> list(final IResultTransformer<T> rowMapper) {
    // closes any open path
    if (this.path != null) {
      join();
    }

    List<T> list;
    if (getTranslator().useSQLPagination()) {
      // defining skip and limit as zero, will default to use SQL paginagion (intead of JDBC pagination).
      list = executor.queryRange(getSql(), rowMapper, 0, 0, this.parameters);
    } else {
      list = executor.queryRange(getSql(), rowMapper, this.skip, this.limit, this.parameters);
    }
    return list;
  }

  public <T> T unique(final IRecordTransformer<T> recordTransformer) {
    return unique(toRowTransformer(recordTransformer));
  }

  /**
   * Executes a query and transform the results according to the transformer.<br>
   * If more than one result is returned an Exception will occur.
   *
   * @param <T>       the bean type
   * @param rowMapper The row transformer
   * @return A collection of transformed results
   */
  public <T> T unique(final IResultTransformer<T> rowMapper) {
    // closes any open path
    if (this.path != null) {
      join();
    }

    return executor.queryUnique(getSql(), rowMapper, this.parameters);
  }

  // ======== SELECT (ONE RESULT) ================

  public <T> T unique(Class<T> klass) {
    QueryMapper mapper = getTranslator().findQueryMapper(klass);
    if (useTree) {
      return select(klass);
    } else {
      return unique(new MapTransformer<>(this, false, klass, mapper));
    }
  }

  public <T> T select(Class<T> klass) {
    return select(klass, true);
  }

  public <T> T select(Class<T> klass, boolean reuse) {
    QueryMapper mapper = getTranslator().findQueryMapper(klass);
    if (useTree) {
      if (reuse) {
        List<T> list = list(new MapTransformer<>(this, true, klass, mapper));

        if (list.size() == 0)
          return null;
        else
          return list.get(0); // first one
      }
    }
    return select(new MapTransformer<>(this, false, klass, mapper));
  }

  public <T> T select(final IRecordTransformer<T> recordTransformer) {
    return select(toRowTransformer(recordTransformer));
  }

  public <T> T select(IResultTransformer<T> rowMapper) {
    int holdMax = this.limit;
    limit(1);

    List<T> list = list(rowMapper);

    limit(holdMax);

    if (list.size() == 0)
      return null;
    else
      return list.get(0); // first one
  }

  /**
   * SQL String. It is cached for multiple access
   */
  @Override
  public String getSql() {
    if (columns.isEmpty()) {
      all();
    }

    return super.getSql();
  }

  public Function subQuery() {
    return Definition.subQuery(this);
  }

  public int paginationColumnOffset() {
    return this.getTranslator().paginationColumnOffset(this);
  }

  @Override
  public Query where(List<Condition> restrictions) {
    return super.where(restrictions);
  }
}
