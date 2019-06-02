package com.github.quintans.ezSQL.dml;

import com.github.quintans.ezSQL.common.api.Value;
import com.github.quintans.ezSQL.db.Association;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.Discriminator;
import com.github.quintans.ezSQL.db.NullSql;
import com.github.quintans.ezSQL.db.Relation;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.translator.Translator;
import com.github.quintans.ezSQL.exception.OrmException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.github.quintans.ezSQL.toolkit.utils.Misc.length;

public abstract class CoreDSL {

  protected Translator translator;

  protected Table table;
  protected String tableAlias;
  protected List<Join> joins;
  protected Condition condition;
  protected Map<String, Object> parameters = new LinkedHashMap<>();

  public static final String JOIN_PREFIX = "j";
  protected AliasBag joinBag = new AliasBag(JOIN_PREFIX);
  protected static final String PREFIX = "t";

  protected String lastFkAlias = null;
  protected Join lastJoin = null;

  protected String lastSql;

  protected List<Condition> discriminatorConditions = null;

  private int rawIndex = 0;


  protected Class<?> lastBeanClass = null;

  protected Map<Column<?>, Function> values;
  protected Column<?>[] sets = null;


  /**
   * list with the associations of the current path
   */
  protected List<PathElement> path = null;

  protected static class PathCondition {
    private List<Function> columns;
    private List<Condition> conditions;

    public List<Function> getColumns() {
      return columns;
    }

    public void setColumns(List<Function> columns) {
      this.columns = columns;
    }

    public List<Condition> getConditions() {
      return this.conditions;
    }

    public void setConditions(List<Condition> conditions) {
      this.conditions = conditions;
    }

    public void addCondition(Condition condition) {
      if (this.conditions == null)
        this.conditions = new ArrayList<>();
      this.conditions.add(condition);
    }

    public void addConditions(List<Condition> conditions) {
      if (this.conditions == null)
        this.conditions = new ArrayList<>();
      this.conditions.addAll(conditions);
    }
  }

  public CoreDSL(Translator translator, Table table) {
    this.translator = translator;

    this.table = table;
    this.tableAlias = PREFIX + "0";

    if (table != null) {
      List<Condition> conditions = table.getConditions();
      if (conditions != null) {
        this.discriminatorConditions = new ArrayList<>(conditions);
        applyWhere(null);
      }
    }
  }

  public Translator getTranslator() {
    return translator;
  }

  public Table getTable() {
    return this.table;
  }

  public String getTableAlias() {
    return this.tableAlias;
  }

  public void setTableAlias(String alias) {
    this.tableAlias = alias;
    this.lastSql = null;
  }

  public List<Join> getJoins() {
    return this.joins;
  }

  public Map<String, Object> getParameters() {
    return this.parameters;
  }

  public Object getParameter(Column<?> column) {
    return this.parameters.get(column.getAlias());
  }

  public Condition getCondition() {
    return this.condition;
  }

  /**
   * Sets the value of parameter to the column.<br>
   * Converts null values to NullSql type or Values interfaces to its value
   *
   * @param col   The column
   * @param value The value to set wrapped in a Parameter
   */
  public void setParameter(Column<?> col, Object value) {
    String name = col.getAlias();

    if (value == null) {
      value = col.getType();
    }

    setParameter(name, value);
  }

  public void setParameter(String name, Object value) {
    if (value instanceof Value<?>) {
      Value<?> e = (Value<?>) value;
      value = e.value();
    }
    this.parameters.put(name, value);
  }

  /**
   * das listas de grupos de Foreign Keys (caminhos),
   * obtem as Foreign Keys correspondentes ao caminho comum mais longo
   * que se consegue percorrer com o grupo de Foreign Keys passado
   *
   * @param cachedAssociation listas de grupos de Foreign Keys (caminhos)
   * @param associations      grupo de Foreign Keys para comparar
   * @return Foreign Keys correspondentes ao caminho comum mais longo que se
   * consegue percorrer
   */
  public static PathElement[] deepestCommonPath(List<PathElement[]> cachedAssociation, List<PathElement> associations) {
    List<PathElement> common = new ArrayList<>();

    if (associations != null) {
      for (PathElement[] path : cachedAssociation) {
        // finds the common start portion in this path
        List<PathElement> temp = new ArrayList<>();
        for (int depth = 0; depth < path.length; depth++) {
          PathElement pe = path[depth];
          if (depth < associations.size()) {
            PathElement pe2 = associations.get(depth);
            if ((pe2.isInner() == null || pe2.isInner().equals(pe.isInner())) && pe2.getBase() != null && pe2.getBase().equals(pe.getBase()))
              temp.add(pe);
            else
              break;
          } else
            break;
        }
        // if common portion is larger than the previous one, use it
        if (temp.size() > common.size()) {
          common = temp;
        }
      }
    }

    return common.toArray(new PathElement[0]);
  }

  public String getAliasForAssociation(Association association) {
    if (this.joinBag != null)
      return this.joinBag.getAlias(association);
    else
      return null;
  }

  /**
   * Indicates that the current association chain should be used to join only.
   * A table end alias can also be supplied.
   *
   * @param paths paths to the Association
   * @param fetch fetch columns
   */
  protected void joinTo(List<PathElement> paths, boolean fetch) {
    if (paths != null) {
      this.addJoin(paths, fetch);

      PathCondition[] cache = buildPathConditions(paths);
      // process the acumulated conditions
      List<Condition> firstConditions = null;
      for (int index = 0; index < cache.length; index++) {
        PathCondition pathCondition = cache[index];
        if (pathCondition != null) {
          List<Condition> conds = pathCondition.getConditions();
          // adjustTableAlias()
          if (conds != null) {
            // index == 0 applies to the starting table
            if (index == 0) {
              // already with the alias applied
              firstConditions = conds;
            } else {
              //addJoin(null, pathCondition.getPath());
              if (firstConditions != null) {
                // add the criterias restriction refering to the table,
                // due to association discriminator
                conds = new ArrayList<>(conds);
                conds.addAll(firstConditions);
                firstConditions = null;
              }
              applyOn(paths.subList(0, index), Definition.and(conds));
            }
          }

          if (pathCondition.getColumns() != null) {
            this.applyInclude(paths.subList(0, index), pathCondition.getColumns());
          }

        }
      }
    }
  }

  private PathCondition[] buildPathConditions(List<PathElement> paths) {
    // see if any targeted table has discriminator columns
    int index = 0;
    List<Condition> tableConditions;
    PathCondition[] cache = new PathCondition[paths.size() + 1];

    // the path condition on position 0 refers the condition on the FROM table
    // both ends of Discriminator conditions (association origin and destination tables) are treated in this block
    for (PathElement pe : paths) {
      index++;

      PathCondition pc = null;
      if (pe.getCondition() != null) {
        pc = new PathCondition();
        pc.addCondition(pe.getCondition());
        cache[index] = pc;
      }

      // table discriminator on target
      tableConditions = pe.getBase().getTableTo().getConditions();
      if (tableConditions != null) {
        if (pc == null) {
          pc = new PathCondition();
          cache[index] = pc;
        }
        cache[index].addConditions(tableConditions);
      }

      // references column Includes
      if (pe.getColumns() != null) {
        if (pc == null) {
          pc = new PathCondition();
          cache[index] = pc;
        }
        pc.setColumns(pe.getColumns());
      }
    }

    // process criterias from the association discriminators
    String fkAlias = this.getTableAlias();
    index = 0;
    for (PathElement pe : paths) {
      index++;
      List<Discriminator> discriminators = pe.getBase().getDiscriminators();
      if (discriminators != null) {
        PathCondition pc = cache[index];
        if (pc == null) {
          pc = new PathCondition();
          cache[index] = pc;
        }

        if (pe.getBase().getDiscriminatorTable().equals(pe.getBase().getTableTo())) {
          for (Discriminator v : discriminators) {
            pc.addCondition(v.getCondition());
          }
        } else {
          // force table alias for the first criteria
          for (Discriminator v : discriminators) {
            Condition crit = v.getCondition();
            crit.setTableAlias(fkAlias);
            pc.addCondition(crit);
          }
        }
      }
      fkAlias = this.joinBag.getAlias(pe.getDerived());
    }

    return cache;
  }

  // keeps the paths(associations) already traversed
  protected List<PathElement[]> cachedAssociation = new ArrayList<>();

  protected List<PathElement> addJoin(List<PathElement> associations, boolean fetch) {
    List<PathElement> local = new ArrayList<>();

    PathElement[] common = deepestCommonPath(this.cachedAssociation, associations);

    if (this.joins == null)
      this.joins = new ArrayList<>();

    // guarda os novos
    // List<ForeignKey> newFks = new ArrayList<ForeignKey>();
    // cria copia, pois os table alias vao ser definidos
    Association[] fks = new Association[associations.size()];
    Association lastFk = null;
    boolean matches = true;
    int f = 0;
    for (PathElement pe : associations) {
      Association association = pe.getBase();
      Association lastCachedFk = null;
      if (matches && f < common.length) {
        if (common[f].getBase().equals(association))
          lastCachedFk = common[f].getDerived();
        else
          matches = false;
      } else
        matches = false;

      if (lastCachedFk == null) {
        // copia para atribuir os alias para esta query
        fks[f] = association.bareCopy();

        /*
        Processes associations.
        The alias of the initial side (from) of the first association is set with firstAlias (main table value).
        The alias of the final sid of the last association, is set with the lastAlias, if it is not null
         */
        String fkAlias;
        if (f == 0) {
          fkAlias = this.tableAlias;
        } else {
          fkAlias = this.joinBag.getAlias(lastFk);
        }
        if (fks[f].isMany2Many()) {
          Association fromFk = fks[f].getFromM2M();
          Association toFk = fks[f].getToM2M();

          prepareAssociation(
              fkAlias,
              this.joinBag.getAlias(fromFk),
              fromFk);

          if (pe.getPreferredAlias() == null) {
            fkAlias = this.joinBag.getAlias(toFk);
          } else {
            fkAlias = pe.getPreferredAlias();
            this.joinBag.setAlias(toFk, pe.getPreferredAlias());
          }

          prepareAssociation(
              this.joinBag.getAlias(fromFk),
              fkAlias,
              toFk);
          lastFk = toFk;
        } else {
          String fkAlias2;

          if (pe.getPreferredAlias() == null) {
            fkAlias2 = this.joinBag.getAlias(fks[f]);
          } else {
            fkAlias2 = pe.getPreferredAlias();
            this.joinBag.setAlias(fks[f], pe.getPreferredAlias());
          }
          prepareAssociation(
              fkAlias,
              fkAlias2,
              fks[f]);
          lastFk = fks[f];
        }

      } else {
        // a lista principal sempre com a associaccao many-to-many
        fks[f] = lastCachedFk;
        // define o fk anterior
        if (fks[f].isMany2Many()) {
          lastFk = fks[f].getToM2M();
        } else
          lastFk = lastCachedFk;
      }
      pe.setDerived(fks[f]);
      local.add(pe); // cache it

      f++;
    }

    // only caches if the path was different
    if (!matches) {
      this.cachedAssociation.add(local.toArray(new PathElement[0]));
    }

    // determina o alias do último join
    this.lastFkAlias = this.joinBag.getAlias(lastFk);

    this.lastJoin = new Join(local, fetch);
    this.joins.add(this.lastJoin);

    return local;
  }

  private void prepareAssociation(String aliasFrom, String aliasTo, Association currentFk) {
    currentFk.setAliasFrom(aliasFrom);
    currentFk.setAliasTo(aliasTo);
    for (Relation rel : currentFk.getRelations()) {
      rel.getFrom().setTableAlias(aliasFrom);
      rel.getTo().setTableAlias(aliasTo);
    }
  }

  protected CoreDSL coreWhere(Condition restriction) {
    List<Condition> conditions = new ArrayList<>();
    conditions.add(restriction);
    coreWhere(conditions);
    return this;
  }

  protected CoreDSL coreWhere(Condition... restrictions) {
    if (restrictions != null) {
      List<Condition> conditions = Arrays.asList(restrictions);
      coreWhere(conditions);
    }
    return this;
  }

  protected CoreDSL coreWhere(List<Condition> restrictions) {
    if (restrictions != null) {
      applyWhere(Definition.and(restrictions));
    }
    return this;
  }

  /**
   * Condition to use in the preceding association
   *
   * @param chain     paths
   * @param condition condition
   */
  protected void applyOn(List<PathElement> chain, Condition condition) {
    if (chain != null && chain.size() > 0) {
      PathElement pe = chain.get(chain.size() - 1);
      Condition copy = (Condition) condition.clone();

      Association fk = pe.getDerived();
      String fkAlias;
      if (fk.isMany2Many()) {
        fkAlias = this.joinBag.getAlias(fk.getToM2M());
      } else {
        fkAlias = this.joinBag.getAlias(pe.getDerived());
      }
      copy.setTableAlias(fkAlias);

      replaceRaw(copy);
      pe.setCondition(copy);

      this.lastSql = null;
    }
  }

  protected void applyInclude(List<PathElement> chain, List<Function> tokens) {
    int len = length(chain);
    if (len > 0) {
      PathElement pe = chain.get(len - 1);
      Association fk = pe.getDerived();
      String fkAlias = this.joinBag.getAlias(fk.isMany2Many() ? fk.getToM2M() : pe.getDerived());
      for (Function token : tokens) {
        token.setTableAlias(fkAlias);
      }

      this.lastSql = null;
    }
  }

  // WHERE ===
  protected void applyWhere(Condition restriction) {
    this.lastSql = null;
    this.condition = null;

    List<Condition> conditions = new ArrayList<>();
    if (this.discriminatorConditions != null) {
      conditions.addAll(this.discriminatorConditions);
    }

    if (restriction != null) {
      Condition cond = (Condition) restriction.clone();
      replaceRaw(cond);
      cond.setTableAlias(this.tableAlias);
      conditions.add(cond);
    }

    if (!conditions.isEmpty()) {
      Condition cond = Definition.and(conditions);
      cond.setTableAlias(this.tableAlias);
      this.condition = cond;
    }
  }

  protected String dumpParameters(Map<String, Object> map) {
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

  public String getSql() {
    if (this.lastSql == null) {
      lastSql = computeSql();
    }
    return this.lastSql;
  }

  protected abstract String computeSql();

  // CONDITIONS

  /**
   * replaces RAW with PARAM
   *
   * @param token token
   */
  protected void replaceRaw(Function token) {
    Function[] members = token.getMembers();
    if (EFunction.RAW.equals(token.getOperator())) {
      String alias = token.getAlias();
      String parameter = "";
      if (this.tableAlias != null) {
        parameter = tableAlias + "_";
      }
      this.rawIndex++;
      if (alias != null) {
        parameter += alias + this.rawIndex;
      } else {
        parameter += "R" + this.rawIndex;
      }
      setParameter(parameter, token.getValue());
      token.setOperator(EFunction.PARAM);
      token.setValue(parameter);
    } else if (EFunction.SUBQUERY.equals(token.getOperator())) {
      QueryDSL subquery = (QueryDSL) token.getValue();
      // copy the parameters of the subquery to the main query
      Map<String, Object> pars = subquery.getParameters();
      for (Entry<String, Object> entry : pars.entrySet()) {
        setParameter(entry.getKey(), entry.getValue());
      }
    } else {
      if (members != null) {
        for (Function member : members) {
          if (member != null) {
            this.replaceRaw(member);
          }
        }
      }
    }
  }


  /**
   * Sets the value by defining a parameter with the column alias
   * This values can be raw values or more elaborated values like
   * UPPER(t0.Column) or AUTO(t0.ID)
   *
   * @param col   The column
   * @param value The value to set
   */
  protected void coreSet(Column<?> col, Object value) {
    if (value == null) {
      value = col.getType();
    } else if (col.getType() == NullSql.CLOB && value instanceof String) {
      value = ((String) value).toCharArray();
    }

    Function token = Function.converteOne(value);
    replaceRaw(token);
    token.setTableAlias(this.tableAlias);

    // if the column was not yet defined, the sql changed
    if (defineParameter(col, token))
      this.lastSql = null;
  }

  protected Object coreGet(Column<?> col) {
    Function tok = this.values.get(col);
    if (tok != null) {
      String key = (String) tok.getValue();
      return this.parameters.get(key);
    }
    return null;
  }

  protected void coreSets(Column<?>... columns) {
    for (Column<?> col : columns) {
      if (!table.equals(col.getTable())) {
        throw new OrmException("Column %s does not belong to table %s", col, table);
      }
      // start by setting columns as null
      coreSet(col, null);
    }
    this.sets = columns;
    this.lastSql = null;
  }

  protected void coreValues(Object... values) {
    if (this.sets == null)
      throw new OrmException("Columns are not defined!");

    if (this.sets.length != values.length)
      throw new OrmException("The number of defined columns is diferent from the number of passed values!");

    int i = 0;
    for (Column<?> col : this.sets) {
      coreSet(col, values[i++]);
    }
  }

  private boolean defineParameter(Column<?> col, Function value) {
    if (!col.getTable().getName().equals(this.table.getName()))
      throw new OrmException("%s does not belong to table %s", col, this.table);

    if (this.values == null)
      this.values = new LinkedHashMap<>();

    Function tok = this.values.put(col, value);
    // if it is a parameter remove it
    if (tok != null) {
      if (EFunction.PARAM.equals(value.getOperator()) && EFunction.PARAM.equals(tok.getOperator())) {
	            /*
	                Replace one param by another
	            */
        String oldKey = (String) tok.getValue();
        String key = (String) value.getValue();
        // change the new param name to the old param name
        value.setValue(tok.getValue());
        // update the old value to the new one
        this.parameters.put(oldKey, this.parameters.get(key));
        // remove the new token
        this.parameters.remove(key);
        // The replace of one param by another should not trigger a new SQL string
        return false;
      } else if (EFunction.PARAM.equals(tok.getOperator())) {
        // removes the previous token
        this.parameters.remove(tok.getValue());
      }
    }

    return true;
  }

}
