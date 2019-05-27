package com.github.quintans.ezSQL.driver;

import com.github.quintans.ezSQL.db.Association;
import com.github.quintans.ezSQL.db.Relation;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.dml.Condition;
import com.github.quintans.ezSQL.dml.Function;
import com.github.quintans.ezSQL.dml.Group;
import com.github.quintans.ezSQL.dml.Join;
import com.github.quintans.ezSQL.dml.PathElement;
import com.github.quintans.ezSQL.dml.Query;
import com.github.quintans.ezSQL.dml.QueryDSL;
import com.github.quintans.ezSQL.dml.Sort;
import com.github.quintans.ezSQL.dml.Union;
import com.github.quintans.ezSQL.toolkit.utils.Appender;

import java.util.ArrayList;
import java.util.List;

import static com.github.quintans.ezSQL.toolkit.utils.Misc.length;

public class GenericQueryBuilder implements QueryBuilder {
  protected QueryDSL<?> query;
  protected Appender columnPart = new Appender(", ");
  protected Appender fromPart = new Appender(", ");
  protected StringBuilder joinPart = new StringBuilder();
  protected Appender wherePart = new Appender(" AND ");
  protected Appender groupPart = new Appender(", ");
  protected StringBuilder havingPart = new StringBuilder();
  protected Appender orderPart = new Appender(", ");
  protected StringBuilder unionPart = new StringBuilder();

  public GenericQueryBuilder(QueryDSL query) {
    this.query = query;
    column();
    if (query.getTable() != null) {
      from();
    } else {
      fromSubQuery();
    }
    where();
    // it is after the where clause because the joins can go to the where clause,
    // and this way the restrictions over the driving table will be applied first

    appendJoins();
    group();
    having();
    union();
    order();
  }

  protected Driver driver() {
    return this.query.getDriver();
  }

  @Override
  public String getColumnPart() {
    return this.columnPart.toString();
  }

  @Override
  public String getFromPart() {
    return this.fromPart.toString();
  }

  @Override
  public String getJoinPart() {
    return this.joinPart.toString();
  }

  @Override
  public String getWherePart() {
    return this.wherePart.toString();
  }

  @Override
  public String getGroupPart() {
    return this.groupPart.toString();
  }

  @Override
  public String getHavingPart() {
    return this.havingPart.toString();
  }

  @Override
  public String getOrderPart() {
    return this.orderPart.toString();
  }

  @Override
  public String getUnionPart() {
    return this.unionPart.toString();
  }

  public void column() {
    int k = 0;
    for (Function token : query.getColumns()) {
      this.columnPart.add(driver().translate(EDml.QUERY, token));
      String a = driver().columnAlias(token, k + 1);
      if (a != null) {
        this.columnPart.append(" AS ", a);
      }
      k++;
    }
  }

  public void from() {
    Table table = query.getTable();
    String alias = query.getTableAlias();
    this.fromPart.addAsOne(driver().tableName(table), " ", driver().tableAlias(alias));
  }

  public void fromSubQuery() {
    QueryDSL subquery = query.getSubquery();
    String alias = query.getAlias();
    this.fromPart.addAsOne("(", driver().getSql(subquery), ")");
    if (alias != null) {
      this.fromPart.append(" ", alias);
    }
  }

  public void joinAssociation(Association fk, boolean inner) {
    if (inner) {
      this.joinPart.append(" INNER JOIN ");
    } else {
      this.joinPart.append(" LEFT OUTER JOIN ");
    }

    this.joinPart.append(driver().tableName(fk.getTableTo()) + " " + fk.getAliasTo() + " ON ");

    Relation[] relations = fk.getRelations();
    for (int i = 0; i < relations.length; i++) {
      if (i > 0) {
        this.joinPart.append(" AND ");
      }
      Relation rel = relations[i];
      this.joinPart.append(driver().translate(EDml.QUERY, rel.getFrom()) +
          " = " +
          driver().translate(EDml.QUERY, rel.getTo()));
    }
  }

  public void joinCriteria(Condition criteria) {
    this.joinPart.append(" AND ").append(driver().translate(EDml.QUERY, criteria));
  }

  public void where() {
    Condition criteria = query.getCondition();
    if (criteria != null) {
      this.wherePart.add(driver().translate(EDml.QUERY, criteria));
    }
  }

  public void group() {
    List<Group> groups = query.getGroupByFunction();
    if (length(groups) > 0) {
      for (Group group : groups) {
        //this.groupPart.Add(this.translator.ColumnAlias(group.Token, group.Position))
        this.groupPart.add(driver().translate(EDml.QUERY, group.getFunction()));
      }
    }
  }

  public void having() {
    Condition having = query.getHaving();
    if (having != null) {
      this.havingPart.append(driver().translate(EDml.QUERY, having));
    }
  }

  public void order() {
    List<Sort> sorts = query.getSorts();
    if (length(sorts) > 0) {
      for (Sort ord : sorts) {
        if (ord.getColumnHolder() != null) {
          this.orderPart.add(driver().translate(EDml.QUERY, ord.getColumnHolder()));
        } else {
          this.orderPart.add(ord.getAlias());
        }

        if (ord.isAsc()) {
          this.orderPart.append(" ASC");
        } else {
          this.orderPart.append(" DESC");
        }
      }
    }
  }

  public void union() {
    List<Union> unions = query.getUnions();
    if (length(unions) > 0) {
      for (Union u : unions) {
        this.unionPart.append(" UNION ");
        if (u.isAll()) {
          this.unionPart.append("ALL ");
        }
        this.unionPart.append(driver().getSql(u.getQuery()));
      }
    }
  }

  /**
   * das listas de grupos de Foreign Keys (caminhos), obtem as Foreign Keys
   * correspondentes ao caminho comum mais longo que se consegue percorrer com
   * o grupo de Foreign Keys passado
   *
   * @param cachedAssociation listas de grupos de Foreign Keys (caminhos)
   * @param associations      grupo de Foreign Keys para comparar
   * @return Foreign Keys correspondentes ao caminho comum mais longo que se
   * consegue percorrer
   */
  protected PathElement[] deepestCommonPath(List<PathElement[]> cachedAssociation, List<PathElement> associations) {
    List<PathElement> common = new ArrayList<PathElement>();

    if (length(associations) > 0) {
      for (PathElement[] path : cachedAssociation) {
        List<PathElement> temp = new ArrayList<PathElement>();
        for (int depth = 0; depth < path.length; depth++) {
          PathElement pe = path[depth];
          if (depth < associations.size()) {
            PathElement pe2 = associations.get(depth);
            if ((pe2.isInner() == null || pe2.isInner().equals(pe.isInner())) && pe2.getBase() != null && pe2.getBase().equals(pe.getBase())) {
              temp.add(pe2);
            } else
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

  protected List<PathElement> reduceAssociations(List<PathElement[]> cachedAssociation, Join join) {
    List<PathElement> associations = join.getPathElements();
    PathElement[] common = deepestCommonPath(cachedAssociation, associations);
    List<PathElement> pes = join.getPathElements();
    cachedAssociation.add(pes.toArray(new PathElement[pes.size()]));
    return associations.subList(common.length, associations.size());
  }

  protected void appendJoins() {
    List<Join> joins = query.getJoins();
    if (length(joins) == 0) {
      return;
    }

    // stores the paths already traverse.
    List<PathElement[]> cachedAssociation = new ArrayList<PathElement[]>();

    for (Join join : joins) {
      /*
       * Example:
       * SELECT *
       * FROM sales
       * INNER JOIN employee
       * ON sales.DepartmentID = employee.DepartmentID
       * AND sales.EmployeeID = employee.EmployeeID
       */

      List<PathElement> associations = reduceAssociations(cachedAssociation, join);
      if (length(associations) > 0) {
        for (PathElement pe : associations) {
          Association association = pe.getDerived();
          if (association.isMany2Many()) {
            // ja' vem com a direccao de navegacao ajustada
            Association fromFk = association.getFromM2M();
            Association toFk = association.getToM2M();

            joinAssociation(fromFk, pe.isInner());
            joinAssociation(toFk, pe.isInner());
          } else {
            joinAssociation(association, pe.isInner());
          }

          if (pe.getCondition() != null) {
            joinCriteria(pe.getCondition());
          }
        }
      }
    }
  }
}
