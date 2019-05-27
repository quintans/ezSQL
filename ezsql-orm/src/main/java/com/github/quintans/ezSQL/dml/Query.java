package com.github.quintans.ezSQL.dml;

import com.github.quintans.ezSQL.AbstractDb;
import com.github.quintans.ezSQL.db.Association;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.transformers.*;
import com.github.quintans.jdbc.SimpleJdbc;
import com.github.quintans.jdbc.exceptions.PersistenceException;
import com.github.quintans.jdbc.transformers.IRowTransformer;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;
import com.github.quintans.jdbc.transformers.SimpleAbstractRowTransformer;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Consumer;

import static com.github.quintans.ezSQL.toolkit.utils.Misc.empty;
import static com.github.quintans.ezSQL.toolkit.utils.Misc.length;

public class Query extends CoreDSL {
  private static final Logger LOG = Logger.getLogger(Query.class);

  public static final String FIRST_RESULT = "first";
  public static final String LAST_RESULT = "last";
  private final AbstractDb db;
  private final JdbcExecutor executor;

  private String alias;
  private Query subquery;
  private boolean distinct;
  private List<Function> columns = new ArrayList<>();
  private List<Sort> sorts;
  private List<Union> unions;
  // saves position of columnHolder
  private int[] groupBy = null;

  private int skip;
  private int limit;

  private Function lastFunction = null;
  private Sort lastSortOn = null;

  private Condition having;

  private boolean useTree;

  public Query(AbstractDb db, Table table) {
    super(db.getDriver(), table);
    this.db = db;
    this.executor = new JdbcExecutor(driver, new SimpleJdbc(db.getJdbcSession()));
  }

  public Query(Query subquery) {
    super(subquery.db.getDriver(), null);
    this.subquery = subquery;
    this.db = subquery.db;
    this.executor = subquery.executor;
    // copy the parameters of the subquery to the main query
    for (Entry<String, Object> entry : subquery.getParameters().entrySet()) {
      this.setParameter(entry.getKey(), entry.getValue());
    }

  }

  public AbstractDb getDb() {
    return db;
  }

  public String getAlias() {
    return alias;
  }

  public boolean isFlat() {
    return !useTree;
  }

  public void alias(String alias) {
    this.alias = alias;
  }

  public void copy(Query other) {
    this.table = other.getTable();
    this.tableAlias = other.getTableAlias();

    if (other.getJoins() != null)
      this.joins = new ArrayList<>(other.getJoins());
    if (other.getCondition() != null)
      this.condition = (Condition) other.getCondition().clone();
    if (this.parameters != null)
      this.parameters = new LinkedHashMap<>(other.getParameters());

    if (other.getSubquery() != null) {
      Query q = other.getSubquery();
      this.subquery = new Query(this.db, q.getTable());
      this.subquery.copy(q);
    }

    this.distinct = other.isDistinct();
    if (other.getColumns() != null)
      this.columns = new ArrayList<>(other.getColumns());
    if (other.getSorts() != null)
      this.sorts = new ArrayList<>(other.getSorts());
    if (other.getUnions() != null)
      this.unions = new ArrayList<>(other.getUnions());
    // saves position of columnHolder
    if (other.getGroupBy() != null)
      this.groupBy = other.getGroupBy().clone();

    this.skip = other.getSkip();
    this.limit = other.getLimit();

    this.lastSql = other.lastSql;
  }

  public int getSkip() {
    return this.skip;
  }

  public Query skip(int firstResult) {
    if (firstResult < 0)
      this.skip = 0;
    else
      this.skip = firstResult;
    return this;
  }

  public int getLimit() {
    return this.limit;
  }

  public Query limit(int maxResults) {
    if (maxResults < 0) {
      this.limit = 0;
    } else {
      this.limit = maxResults;
    }
    return this;
  }

  public Query getSubquery() {
    return this.subquery;
  }

  public Query distinct() {
    this.distinct = true;
    this.lastSql = null;
    return this;
  }

  public boolean isDistinct() {
    return this.distinct;
  }

  // COLUMN ===

  public Query all() {
    if (this.table != null) {
      for (Column<?> column : this.table.getColumns()) {
        column(column);
      }
    }
    return this;
  }

  public Query column(Object... cols) {
    if (cols != null && cols.length > 0) {
      for (Object col : cols) {
        this.lastFunction = Function.converteOne(col);
        replaceRaw(lastFunction);
        this.lastFunction.setTableAlias(this.tableAlias);
        this.columns.add(this.lastFunction);
      }
    }

    this.lastSql = null;

    return this;
  }

  public Query count() {
    column(Definition.count());
    return this;
  }

  public Query count(Object... expr) {
    if (expr != null && expr.length > 0) {
      for (Object e : expr) {
        column(Definition.count(e));
      }
    }
    return this;
  }

  /**
   * Defines the alias of the last column, or if none was defined, defines the table alias;
   *
   * @param alias The Alias
   * @return The query
   */
  public Query as(String alias) {
    if (this.lastFunction != null) {
      this.lastFunction.as(alias);
    } else if (this.path != null) {
      this.path.get(this.path.size() - 1).setPreferredAlias(alias);
    } else {
      this.joinBag = new AliasBag(alias + "_" + JOIN_PREFIX);
      this.tableAlias = alias;
    }

    this.lastSql = null;

    return this;
  }

  // ===

  public List<Function> getColumns() {
    return this.columns;
  }

  // WHERE ===
  public Query where(Condition restriction) {
    return (Query) super.coreWhere(restriction);
  }

  public Query where(Condition... restrictions) {
    return (Query) super.coreWhere(restrictions);
  }

  public Query where(List<Condition> restrictions) {
    return (Query) super.coreWhere(restrictions);
  }

  // ===

  // ORDER ===
  private Query addOrder(Sort sort) {
    if (this.sorts == null)
      this.sorts = new ArrayList<>();

    this.sorts.add(sort);

    this.lastSql = null;

    return this;
  }

  /**
   * Apply sorting.<br>
   * If it is over an alias it will apply immediately without further check.<br>
   * If it is over a column it will check against all the tables in the query, applying it to the first match.<br>
   * If you want to addOrder by a column from the table targeted by the last association, use orderOn
   *
   * @param sorts the sorts
   * @return the query
   */
  public Query orderBy(Sort... sorts) {
    for (Sort sort : sorts) {
      if (sort.getAlias() != null) {
        return addOrder(sort);
      }

      Column<?> column = sort.getColumnHolder().getColumn();
      String tableName = column.getTable().getName();
      if (tableName.equals(getTable().getName())) {
        return orderOn(sort, tableAlias);
      }
      // find first table matching the column
      for (Join join : this.joins) {
        for (PathElement pe : join.getPathElements()) {
          Association derived = pe.getDerived();
          if (derived.getTableTo().getName().equals(tableName)) {
            return orderOn(sort, derived.getAliasTo());
          }
        }
      }
      throw new PersistenceException("Column " + column + " does belong to " + table + " table or any table in existing joins");
    }
    return this;
  }

  public Query orderOn(Sort sort, String alias) {
    Sort other = sort.toBuilder().build();
    ColumnHolder ch = other.getColumnHolder();
    if (alias != null) {
      ch.setTableAlias(alias);
    } else {
      ch.setTableAlias(this.tableAlias);
    }

    return addOrder(other);
  }

  /**
   * Sort by column belonging to another table.
   * This might come in handy when the orderBy cannot be declared in the same orderBy as the joins.
   *
   * @param sort         a coluna de ordenação
   * @param associations associações para chegar à tabela que contem a coluna de
   *                     ordenação
   * @return devolve a query
   */
  public Query orderOn(Sort sort, Association... associations) {
    if (empty(associations)) {
      throw new IllegalArgumentException("Associations cannot be empty");
    }
    List<PathElement> pathElements = new ArrayList<>();
    for (Association association : associations)
      pathElements.add(new PathElement(association, null));

    return orderOn(sort, pathElements);
  }

  private Query orderOn(Sort sort, List<PathElement> pathElements) {
    PathElement[] common = deepestCommonPath(this.cachedAssociation, pathElements);
    if (common.length == pathElements.size()) {
      return orderOn(sort, pathElementAlias(common[common.length - 1]));
    } else
      throw new PersistenceException("The path specified in the orderBy is not valid");

  }

  /**
   * Define a coluna a ordenar. A coluna pertence à última associação
   * definida, <br>
   * ou se não houver nenhuma associação, a coluna pertencente à tabela
   *
   * @param sort the sort
   * @return current query
   */
  public Query orderOn(Sort sort) {
    if (this.path != null) {
      PathElement last = this.path.get(this.path.size() - 1);
      if (last.getOrders() == null) {
        last.setOrders(new ArrayList<>());
      }
      // delay adding orderBy
      this.lastSortOn = sort;
      last.getOrders().add(this.lastSortOn);
      return this;
    } else if (this.lastJoin != null)
      return orderOn(sort, this.lastJoin.getPathElements());
    else
      return orderOn(sort, this.lastFkAlias);
  }

  public List<Sort> getSorts() {
    return this.sorts;
  }

  // ===

  // JOINS ===


  /**
   * includes the associations as inner joins to the current path
   *
   * @param inner
   * @param associations
   * @return
   */
  private Query _associate(boolean inner, Association... associations) {
    if (length(associations) == 0) {
      throw new PersistenceException("Inner cannot be used with an empty association list!");
    }

    if (this.path == null)
      this.path = new ArrayList<>();

    Table lastTable;
    if (path.size() > 0) {
      lastTable = path.get(path.size() - 1).getBase().getTableTo();
    } else {
      lastTable = table;
    }
    // all associations must be linked by table
    for (Association assoc : associations) {
      if (!lastTable.equals(assoc.getTableFrom())) {
        StringBuilder sb = new StringBuilder();
        sb.append("Association list ");
        for (Association a : associations) {
          sb.append("[")
              .append(a.genericPath())
              .append("]");
        }
        sb.append(" is invalid. Association [")
            .append(assoc.genericPath())
            .append("] must start on table ")
            .append(lastTable.getName());
        throw new PersistenceException(sb.toString());
      }
      lastTable = assoc.getTableTo();
    }

    for (Association association : associations) {
      PathElement pe = new PathElement(association, inner);
      this.path.add(pe);
    }

    this.lastSql = null;

    return this;
  }

  /**
   * includes the associations as inner joins to the current path.
   *
   * @param associations
   * @return
   */
  public Query inner(Association... associations) {
    return _associate(true, associations);
  }

  /**
   * includes the associations as outer joins to the current path
   *
   * @param associations
   * @return
   */
  public Query outer(Association... associations) {
    return _associate(false, associations);
  }

  private void _fetch(List<PathElement> paths) {
    useTree = true;

    if (paths != null) {
      for (PathElement pe : paths) {
        // includes all columns if there wasn't a previous include
        includeInPath(pe);
      }
    }

    _join(true);
  }

  /**
   * This will trigger a result that can be dumped in a tree object
   * using current association path to build the tree result.<br>
   * If no columns where included in this path, it will includes all the columns
   * of all the tables referred by the association path.
   *
   * @return
   */
  public Query fetch() {
    _fetch(this.path);

    return this;
  }

  /**
   * This will NOT trigger a result that can be dumped in a tree object.<br>
   * Any included column, will be considered as belonging to the root object.
   *
   * @return
   */
  public Query join() {
    _join(false);
    return this;
  }

  private void _join(boolean fetch) {
    if (this.path != null) {
      List<Function> tokens = new ArrayList<>();
      for (PathElement pe : this.path) {
        List<Function> funs = pe.getColumns();
        if (funs != null) {
          for (Function fun : funs) {
            tokens.add(fun);
            if (!fetch) {
              fun.setPseudoTableAlias(this.tableAlias);
            }
          }
        }
      }

      this.columns.addAll(tokens);
    }

    // only after this the joins will have the proper join table alias
    super.joinTo(this.path, fetch);

    // process pending sorts
    if (this.path != null) {
      for (PathElement pe : this.path) {
        if (pe.getOrders() != null) {
          for (Sort o : pe.getOrders()) {
            o.getColumnHolder().setTableAlias(pathElementAlias(pe));
            this.getSorts().add(o);
          }
        }
      }
    }
    this.path = null;
    this.lastSql = null;
  }

  private static String pathElementAlias(PathElement pe) {
    Association derived = pe.getDerived();
    if (derived.isMany2Many()) {
      return derived.getToM2M().getAliasTo();
    } else {
      return derived.getAliasTo();
    }
  }

  /**
   * The same as inner(...).join()
   *
   * @param associations
   * @return
   */
  public Query innerJoin(Association... associations) {
    return inner(associations).join();
  }

  /**
   * The same as outer(...).join()
   *
   * @param associations
   * @return
   */
  public Query outerJoin(Association... associations) {
    return outer(associations).join();
  }

  /**
   * Includes any kind of column (table column or function)
   * referring to the table targeted by the last association.
   *
   * @param columns or functions
   * @return
   */
  public Query include(Object... columns) {
    int lenPath = length(this.path);
    if (lenPath > 0) {
      PathElement lastPath = this.path.get(lenPath - 1);
      Table lastTable = lastPath.getBase().getTableTo();
      for (Object c : columns) {
        // if it is a columns check if it belongs to the last table
        if (c instanceof Column) {
          Column<?> col = (Column<?>) c;
          if (!col.getTable().equals(lastTable)) {
            throw new PersistenceException(
                String.format("Column %s does not belong to the table target by the association %s.",
                    col.toString(),
                    lastPath.getBase().genericPath()));
          }
        }
      }
      includeInPath(lastPath, columns);

      this.lastSql = null;
    } else {
      throw new PersistenceException("There is no current join");
    }
    return this;
  }

  private void includeInPath(PathElement lastPath, Object... columns) {
    if (length(columns) > 0 || length(lastPath.getColumns()) == 0) {
      if (length(columns) == 0) {
        // use all columns of the targeted table
        columns = lastPath.getBase().getTableTo().getColumns().toArray();
      }
      List<Function> toks = lastPath.getColumns();
      if (toks == null) {
        toks = new ArrayList<>();
        lastPath.setColumns(toks);
      }
      for (Object c : columns) {
        this.lastFunction = Function.converteOne(c);
        toks.add(this.lastFunction);
      }
    }
  }

  /**
   * includes all column from the table from the last association but the ones declared in this method.
   *
   * @param columns
   * @return
   */
  public Query exclude(Column<?>... columns) {
    int lenPath = length(this.path);
    if (lenPath > 0) {
      if (length(columns) == 0) {
        throw new PersistenceException("null or empty values was passed");
      }
      Set<Column<?>> cols = this.path.get(lenPath - 1).getBase().getTableTo().getColumns();
      LinkedHashSet<Column<?>> remain = new LinkedHashSet<>(cols);
      for (Column<?> c : columns) {
        remain.remove(c);
      }

      include(remain.toArray());
    } else {
      throw new PersistenceException("There is no current join");
    }
    return this;
  }
  /* INCLUDES */

  /**
   * Executa um OUTER join com as tabelas definidas pelas foreign keys.<br>
   * TODAS as colunas das tabelas intermédias são incluidas no select bem como
   * a TODAS as colunas da tabela no fim das associações.<br>
   *
   * @param associations as foreign keys que definem uma navegação
   * @return
   */
  public Query outerFetch(Association... associations) {
    outer(associations).fetch();
    return this;
  }

  /**
   * Executa um INNER join com as tabelas definidas pelas foreign keys.<br>
   * TODAS as colunas das tabelas intermédias são incluidas no select bem como
   * TODAS as colunas da tabela no fim das associações.<br>
   *
   * @param associations as foreign keys que definem uma navegação
   * @return
   */
  public Query innerFetch(Association... associations) {
    inner(associations).fetch();
    return this;
  }

  /**
   * Restriction to get to the previous association
   *
   * @param condition Restriction
   * @return
   */
  public Query on(Condition... condition) {
    int lenPath = length(this.path);
    if (lenPath > 0) {
      Condition retriction;
      if (length(condition) == 0) {
        throw new PersistenceException("null or empty criterias was passed");
      } else {
        retriction = Definition.and(condition);
      }
      this.path.get(lenPath - 1).setCondition(retriction);

      this.lastSql = null;
    } else {
      throw new PersistenceException("There is no current join");
    }
    return this;
  }

  // =====

  // UNIONS ===
  public Query union(Query query) {
    if (this.unions == null)
      this.unions = new ArrayList<>();
    this.unions.add(new Union(query, false));

    this.lastSql = null;

    return this;
  }

  public Query unionAll(Query query) {
    if (this.unions == null)
      this.unions = new ArrayList<>();
    this.unions.add(new Union(query, true));

    this.lastSql = null;

    return this;
  }

  public List<Union> getUnions() {
    return this.unions;
  }

  // ===

  // GROUP BY ===
  public Query groupByUntil(int untilPos) {
    int[] pos = new int[untilPos];
    for (int i = 0; i < pos.length; i++)
      pos[i] = i + 1;

    this.groupBy = pos;

    this.lastSql = null;

    return this;
  }

  public Query groupBy(int... pos) {
    this.groupBy = pos;

    this.lastSql = null;

    return this;
  }

  public int[] getGroupBy() {
    return this.groupBy;
  }

  public List<Group> getGroupByFunction() {
    List<Group> groups = null;
    int length = this.groupBy == null ? 0 : groupBy.length;
    if (length > 0) {
      groups = new ArrayList<>(length);
      for (int k = 0; k < length; k++) {
        int idx = groupBy[k] - 1;
        groups.add(new Group(idx, columns.get(idx)));
      }
    }
    return groups;
  }

  public Query groupBy(Column<?>... cols) {
    this.lastSql = null;

    if (cols == null || cols.length == 0) {
      this.groupBy = null;
      return this;
    }

    this.groupBy = new int[cols.length];

    int pos = 1;
    for (int i = 0; i < cols.length; i++) {
      for (Function function : this.columns) {
        if (function instanceof ColumnHolder) {
          ColumnHolder ch = (ColumnHolder) function;
          if (ch.getColumn().equals(cols[i])) {
            this.groupBy[i] = pos;
            break;
          }
        }
      }
      pos++;

      if (this.groupBy[i] == 0)
        throw new PersistenceException(String.format("Column alias '%s' was not found", cols[i]));
    }

    return this;
  }

  public Query groupBy(String... aliases) {
    this.lastSql = null;

    if (aliases == null || aliases.length == 0) {
      this.groupBy = null;
      return this;
    }

    this.groupBy = new int[aliases.length];

    int pos = 1;
    for (int i = 0; i < aliases.length; i++) {
      for (Function function : this.columns) {
        if (aliases[i].equals(function.getAlias())) {
          this.groupBy[i] = pos;
          break;
        }
      }
      pos++;

      if (this.groupBy[i] == 0)
        throw new PersistenceException(String.format("Column alias '%s' was not found", aliases[i]));
    }

    return this;
  }

  public Condition getHaving() {
    return having;
  }

  /**
   * Adds a Having clause to the query. The tokens are not processed. You will
   * have to explicitly set all table alias.
   *
   * @param having
   * @return this
   */
  public Query having(Condition... having) {
    if (having != null) {
      this.having = Definition.and(having);
      this.replaceAlias(this.having);
    }

    return this;
  }

  /**
   * replaces ALIAS with the respective select parcel
   *
   * @param token
   */
  private void replaceAlias(Function token) {
    Function[] members = token.getMembers();
    if (EFunction.ALIAS.equals(token.getOperator())) {
      String alias = (String) token.getValue();
      for (Function v : columns) {
        // full copies the matching
        if (alias.equals(v.getAlias())) {
          token.as(alias);
          token.setMembers(v.getMembers());
          token.setOperator(v.getOperator());
          token.setTableAlias(v.getTableAlias());
          token.setValue(v.getValue());
          break;
        }
      }
    } else {
      if (members != null) {
        for (Function t : members) {
          if (t != null) {
            this.replaceAlias(t);
          }
        }
      }
    }
  }

  // ======== RETRIVE ==============

  public void run(final Consumer<Record> processor) {
    list(createTransformer(processor));
  }

  public void runOne(final Consumer<Record> processor) {
    fetchUnique(createTransformer(processor));
  }

  public void runFirst(final Consumer<Record> processor) {
    select(createTransformer(processor));
  }

  private AbstractDbRowTransformer<Void> createTransformer(final Consumer<Record> processor) {
    return new AbstractDbRowTransformer<Void>() {
      @Override
      public Void transform(ResultSetWrapper rsw) throws SQLException {
        processor.accept(new Record(Query.this, rsw));
        return null;
      }
    };
  }

  public List<Object[]> listRaw(final Class<?>... clazzes) {
    return list(createRawTransformer(clazzes));
  }

  public Object[] uniqueRaw(final Class<?>... clazzes) {
    return fetchUnique(createRawTransformer(clazzes));
  }

  private SimpleAbstractDbRowTransformer<Object[]> createRawTransformer(final Class<?>... clazzes) {
    if (length(clazzes) == 0) {
      throw new PersistenceException("Classes must be defined!");
    }

    final int offset = paginationColumnOffset();

    return new SimpleAbstractDbRowTransformer<Object[]>() {
      @Override
      public Object[] transform(ResultSetWrapper rsw) throws SQLException {
        return Query.this.transform(rsw, offset, clazzes);
      }
    };
  }

  private Object[] transform(ResultSetWrapper rsw, int offset, Class<?>... clazzes) throws SQLException {
    int[] columnTypes = rsw.getColumnTypes();
    int cnt;
    if (clazzes.length > 0)
      cnt = Math.min(columnTypes.length, clazzes.length);
    else
      cnt = columnTypes.length;
    Object objs[] = new Object[cnt];
    for (int i = 0; i < cnt; i++) {
      objs[i] = getDriver().fromDb(rsw, i + 1 + offset, clazzes[i]);
    }
    return objs;
  }


  /**
   * Retrives a collection of objects of simple type (not beans). Ex: Boolean,
   * String, enum, ...
   *
   * @param clazz class of the object to return
   * @return
   */
  public <T> List<T> listRaw(final Class<T> clazz) {
    final int offset = paginationColumnOffset();

    return list(new SimpleAbstractDbRowTransformer<T>() {
      @Override
      public T transform(ResultSetWrapper rsw) throws SQLException {
        return getDriver().fromDb(rsw, 1 + offset, clazz);
      }
    });
  }

  public <T> T uniqueRaw(final Class<T> clazz) {
    final int offset = paginationColumnOffset();

    return fetchUnique(new SimpleAbstractDbRowTransformer<T>() {
      @Override
      public T transform(ResultSetWrapper rsw) throws SQLException {
        return getDriver().fromDb(rsw, 1 + offset, clazz);
      }
    });
  }

  public Boolean uniqueBoolean() {
    final int offset = paginationColumnOffset();

    return fetchUnique(new SimpleAbstractDbRowTransformer<Boolean>() {
      @Override
      public Boolean transform(ResultSetWrapper rsw) throws SQLException {
        return getDriver().toBoolean(rsw, 1 + offset);
      }
    });
  }

  public Integer uniqueInteger() {
    final int offset = paginationColumnOffset();

    return fetchUnique(new SimpleAbstractRowTransformer<Integer>() {
      @Override
      public Integer transform(ResultSetWrapper rsw) throws SQLException {
        return rsw.getResultSet().getInt(1 + offset);
      }
    });
  }

  public Long uniqueLong() {
    final int offset = paginationColumnOffset();

    return fetchUnique(new SimpleAbstractRowTransformer<Long>() {
      @Override
      public Long transform(ResultSetWrapper rsw) throws SQLException {
        return rsw.getResultSet().getLong(1 + offset);
      }
    });
  }

  public Float uniqueFloat() {
    final int offset = paginationColumnOffset();

    return fetchUnique(new SimpleAbstractRowTransformer<Float>() {
      @Override
      public Float transform(ResultSetWrapper rsw) throws SQLException {
        return rsw.getResultSet().getFloat(1 + offset);
      }
    });
  }

  public Double uniqueDouble() {
    final int offset = paginationColumnOffset();

    return fetchUnique(new SimpleAbstractRowTransformer<Double>() {
      @Override
      public Double transform(ResultSetWrapper rsw) throws SQLException {
        return rsw.getResultSet().getDouble(1 + offset);
      }
    });
  }

  public String uniqueString() {
    final int offset = paginationColumnOffset();
    return fetchUnique(new SimpleAbstractRowTransformer<String>() {
      @Override
      public String transform(ResultSetWrapper rsw) throws SQLException {
        return rsw.getResultSet().getString(1 + offset);
      }
    });
  }

  public BigDecimal uniqueBigDecimal() {
    final int offset = paginationColumnOffset();
    return fetchUnique(new SimpleAbstractRowTransformer<BigDecimal>() {
      @Override
      public BigDecimal transform(ResultSetWrapper rsw) throws SQLException {
        return rsw.getResultSet().getBigDecimal(1 + offset);
      }
    });
  }

  private <T> T fetchUnique(IRowTransformer<T> rt) {
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
    return list(new MapTransformer<>(this, reuse, klass, getDriver().findQueryMapper(klass)));
  }


  /**
   * Executes a query and transform the results to the bean type passed as
   * parameter,<br>
   * matching the alias with bean property name. If no alias is supplied, it
   * is used the column alias.
   *
   * @param klass
   * @return
   */
  public <T> List<T> list(Class<T> klass) {
    return list(klass, true);
  }

  private <T> IRowTransformer<T> toRowTransformer(final IRecordTransformer<T> recordTransformer) {
    return new SimpleAbstractRowTransformer<T>() {
      @Override
      public T transform(ResultSetWrapper rsw) throws SQLException {
        return recordTransformer.transform(new Record(Query.this, rsw));
      }
    };
  }

  /**
   * Executes a query and transforms the row with a record transformer
   *
   * @param recordTransformer
   * @param <T>
   * @return
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
  public <T> List<T> list(final IRowTransformer<T> rowMapper) {
    // closes any open path
    if (this.path != null) {
      join();
    }

    List<T> list;
    if (getDriver().useSQLPagination()) {
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
  public <T> T unique(final IRowTransformer<T> rowMapper) {
    // closes any open path
    if (this.path != null) {
      join();
    }

    return executor.queryUnique(getSql(), rowMapper, this.parameters);
  }

  // ======== SELECT (ONE RESULT) ================

  public <T> T unique(Class<T> klass) {
    QueryMapper mapper = getDriver().findQueryMapper(klass);
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
    QueryMapper mapper = getDriver().findQueryMapper(klass);
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

  public <T> T select(IRowTransformer<T> rowMapper) {
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

  @Override
  protected String computeSql() {
    return getDriver().getSql(this);
  }

  public Function subQuery() {
    return Definition.subQuery(this);
  }

  public int paginationColumnOffset() {
    return this.getDriver().paginationColumnOffset(this);
  }
}
