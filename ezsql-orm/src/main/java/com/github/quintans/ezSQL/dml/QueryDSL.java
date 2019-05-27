package com.github.quintans.ezSQL.dml;

import com.github.quintans.ezSQL.db.Association;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.jdbc.exceptions.PersistenceException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.github.quintans.ezSQL.toolkit.utils.Misc.empty;
import static com.github.quintans.ezSQL.toolkit.utils.Misc.length;

public class QueryDSL<T extends QueryDSL> extends CoreDSL {
  public static final String FIRST_RESULT = "first";
  public static final String LAST_RESULT = "last";

  protected String alias;
  protected QueryDSL subquery;
  protected boolean distinct;
  protected List<Function> columns = new ArrayList<>();
  protected List<Sort> sorts;
  protected List<Union> unions;
  // saves position of columnHolder
  protected int[] groupBy = null;

  protected int skip;
  protected int limit;

  protected Function lastFunction = null;
  protected Sort lastSortOn = null;

  protected Condition having;

  protected boolean useTree;

  public QueryDSL(Driver driver, Table table) {
    super(driver, table);
  }

  public QueryDSL(QueryDSL subquery) {
    super(subquery.getDriver(), null);
    this.subquery = subquery;
    // copy the parameters of the subquery to the main query
    for (Map.Entry<String, Object> entry : subquery.getParameters().entrySet()) {
      this.setParameter(entry.getKey(), entry.getValue());
    }
  }

  @Override
  protected String computeSql() {
    return getDriver().getSql(this);
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

  public void copy(QueryDSL other) {
    this.table = other.getTable();
    this.tableAlias = other.getTableAlias();

    if (other.getJoins() != null)
      this.joins = new ArrayList<>(other.getJoins());
    if (other.getCondition() != null)
      this.condition = (Condition) other.getCondition().clone();
    if (this.parameters != null)
      this.parameters = new LinkedHashMap<>(other.getParameters());

    if (other.getSubquery() != null) {
      QueryDSL q = other.getSubquery();
      this.subquery = new QueryDSL(this.driver, q.getTable());
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

  @SuppressWarnings("unchecked")
  public  T skip(int firstResult) {
    if (firstResult < 0)
      this.skip = 0;
    else
      this.skip = firstResult;
    return (T) this;
  }

  public int getLimit() {
    return this.limit;
  }

  @SuppressWarnings("unchecked")
  public  T limit(int maxResults) {
    if (maxResults < 0) {
      this.limit = 0;
    } else {
      this.limit = maxResults;
    }
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public  T getSubquery() {
    return (T) this.subquery;
  }

  @SuppressWarnings("unchecked")
  public  T distinct() {
    this.distinct = true;
    this.lastSql = null;
    return (T) this;
  }

  public boolean isDistinct() {
    return this.distinct;
  }

  // COLUMN ===

  @SuppressWarnings("unchecked")
  public  T all() {
    if (this.table != null) {
      for (Column<?> column : this.table.getColumns()) {
        column(column);
      }
    }
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public  T column(Object... cols) {
    if (cols != null && cols.length > 0) {
      for (Object col : cols) {
        this.lastFunction = Function.converteOne(col);
        replaceRaw(lastFunction);
        this.lastFunction.setTableAlias(this.tableAlias);
        this.columns.add(this.lastFunction);
      }
    }

    this.lastSql = null;

    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public  T count() {
    column(Definition.count());
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public  T count(Object... expr) {
    if (expr != null && expr.length > 0) {
      for (Object e : expr) {
        column(Definition.count(e));
      }
    }
    return (T) this;
  }

  /**
   * Defines the alias of the last column, or if none was defined, defines the table alias;
   *
   * @param alias The Alias
   * @return The query
   */
  @SuppressWarnings("unchecked")
  public  T as(String alias) {
    if (this.lastFunction != null) {
      this.lastFunction.as(alias);
    } else if (this.path != null) {
      this.path.get(this.path.size() - 1).setPreferredAlias(alias);
    } else {
      this.joinBag = new AliasBag(alias + "_" + JOIN_PREFIX);
      this.tableAlias = alias;
    }

    this.lastSql = null;

    return (T) this;
  }

  // ===

  public List<Function> getColumns() {
    return this.columns;
  }

  // WHERE ===
  @SuppressWarnings("unchecked")
  public  T where(Condition restriction) {
    return (T) super.coreWhere(restriction);
  }

  @SuppressWarnings("unchecked")
  public  T where(Condition... restrictions) {
    return (T) super.coreWhere(restrictions);
  }

  @SuppressWarnings("unchecked")
  public  T where(List<Condition> restrictions) {
    return (T) super.coreWhere(restrictions);
  }

  // ===

  // ORDER ===
  @SuppressWarnings("unchecked")
  private  T addOrder(Sort sort) {
    if (this.sorts == null)
      this.sorts = new ArrayList<>();

    this.sorts.add(sort);

    this.lastSql = null;

    return (T) this;
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
  @SuppressWarnings("unchecked")
  public  T orderBy(Sort... sorts) {
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
    return (T) this;
  }

  public  T orderOn(Sort sort, String alias) {
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
  public  T orderOn(Sort sort, Association... associations) {
    if (empty(associations)) {
      throw new IllegalArgumentException("Associations cannot be empty");
    }
    List<PathElement> pathElements = new ArrayList<>();
    for (Association association : associations)
      pathElements.add(new PathElement(association, null));

    return orderOn(sort, pathElements);
  }

  private  T orderOn(Sort sort, List<PathElement> pathElements) {
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
  @SuppressWarnings("unchecked")
  public  T orderOn(Sort sort) {
    if (this.path != null) {
      PathElement last = this.path.get(this.path.size() - 1);
      if (last.getOrders() == null) {
        last.setOrders(new ArrayList<>());
      }
      // delay adding orderBy
      this.lastSortOn = sort;
      last.getOrders().add(this.lastSortOn);
      return (T) this;
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
  @SuppressWarnings("unchecked")
  private  T myAssociate(boolean inner, Association... associations) {
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

    return (T) this;
  }

  /**
   * includes the associations as inner joins to the current path.
   *
   * @param associations
   * @return
   */
  public  T inner(Association... associations) {
    return myAssociate(true, associations);
  }

  /**
   * includes the associations as outer joins to the current path
   *
   * @param associations
   * @return
   */
  public  T outer(Association... associations) {
    return myAssociate(false, associations);
  }

  private void myFetch(List<PathElement> paths) {
    useTree = true;

    if (paths != null) {
      for (PathElement pe : paths) {
        // includes all columns if there wasn't a previous include
        includeInPath(pe);
      }
    }

    myJoin(true);
  }

  /**
   * This will trigger a result that can be dumped in a tree object
   * using current association path to build the tree result.<br>
   * If no columns where included in this path, it will includes all the columns
   * of all the tables referred by the association path.
   *
   * @return
   */
  @SuppressWarnings("unchecked")
  public  T fetch() {
    myFetch(this.path);

    return (T) this;
  }

  /**
   * This will NOT trigger a result that can be dumped in a tree object.<br>
   * Any included column, will be considered as belonging to the root object.
   *
   * @return
   */
  @SuppressWarnings("unchecked")
  public  T join() {
    myJoin(false);
    return (T) this;
  }

  private void myJoin(boolean fetch) {
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
  @SuppressWarnings("unchecked")
  public  T innerJoin(Association... associations) {
    return (T) inner(associations).join();
  }

  /**
   * The same as outer(...).join()
   *
   * @param associations
   * @return
   */
  @SuppressWarnings("unchecked")
  public  T outerJoin(Association... associations) {
    return (T) outer(associations).join();
  }

  /**
   * Includes any kind of column (table column or function)
   * referring to the table targeted by the last association.
   *
   * @param columns or functions
   * @return
   */
  @SuppressWarnings("unchecked")
  public  T include(Object... columns) {
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
    return (T) this;
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
  @SuppressWarnings("unchecked")
  public  T exclude(Column<?>... columns) {
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
    return (T) this;
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
  @SuppressWarnings("unchecked")
  public  T outerFetch(Association... associations) {
    outer(associations).fetch();
    return (T) this;
  }

  /**
   * Executa um INNER join com as tabelas definidas pelas foreign keys.<br>
   * TODAS as colunas das tabelas intermédias são incluidas no select bem como
   * TODAS as colunas da tabela no fim das associações.<br>
   *
   * @param associations as foreign keys que definem uma navegação
   * @return
   */
  @SuppressWarnings("unchecked")
  public  T innerFetch(Association... associations) {
    inner(associations).fetch();
    return (T) this;
  }

  /**
   * Restriction to get to the previous association
   *
   * @param condition Restriction
   * @return
   */
  @SuppressWarnings("unchecked")
  public  T on(Condition... condition) {
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
    return (T) this;
  }

  // =====

  // UNIONS ===
  @SuppressWarnings("unchecked")
  public  T union(QueryDSL query) {
    if (this.unions == null)
      this.unions = new ArrayList<>();
    this.unions.add(new Union(query, false));

    this.lastSql = null;

    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public  T unionAll(QueryDSL query) {
    if (this.unions == null)
      this.unions = new ArrayList<>();
    this.unions.add(new Union(query, true));

    this.lastSql = null;

    return (T) this;
  }

  public List<Union> getUnions() {
    return this.unions;
  }

  // ===

  // GROUP BY ===
  @SuppressWarnings("unchecked")
  public  T groupByUntil(int untilPos) {
    int[] pos = new int[untilPos];
    for (int i = 0; i < pos.length; i++)
      pos[i] = i + 1;

    this.groupBy = pos;

    this.lastSql = null;

    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public  T groupBy(int... pos) {
    this.groupBy = pos;

    this.lastSql = null;

    return (T) this;
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

  @SuppressWarnings("unchecked")
  public  T groupBy(Column<?>... cols) {
    this.lastSql = null;

    if (cols == null || cols.length == 0) {
      this.groupBy = null;
      return (T) this;
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

    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public  T groupBy(String... aliases) {
    this.lastSql = null;

    if (aliases == null || aliases.length == 0) {
      this.groupBy = null;
      return (T) this;
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

    return (T) this;
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
  @SuppressWarnings("unchecked")
  public  T having(Condition... having) {
    if (having != null) {
      this.having = Definition.and(having);
      this.replaceAlias(this.having);
    }

    return (T) this;
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


}
