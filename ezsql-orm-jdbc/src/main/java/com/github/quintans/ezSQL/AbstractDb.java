package com.github.quintans.ezSQL;

import com.github.quintans.ezSQL.common.api.Updatable;
import com.github.quintans.ezSQL.db.Association;
import com.github.quintans.ezSQL.db.Relation;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.dml.Condition;
import com.github.quintans.ezSQL.dml.Definition;
import com.github.quintans.ezSQL.dml.Delete;
import com.github.quintans.ezSQL.dml.Insert;
import com.github.quintans.ezSQL.dml.Query;
import com.github.quintans.ezSQL.dml.Update;
import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.toolkit.reflection.FieldUtils;
import com.github.quintans.ezSQL.toolkit.reflection.TypedField;
import com.github.quintans.ezSQL.translator.Translator;
import com.github.quintans.jdbc.JdbcSession;
import com.github.quintans.jdbc.exceptions.PersistenceException;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public abstract class AbstractDb {

  private Translator translator;
  private final Driver driver;
  private final JdbcSession jdbcSession;

  public AbstractDb(Translator translator, Driver driver) {
    this.translator = translator;
    this.driver = driver;
    this.jdbcSession = new DbJdbcSession(this, driver.isPmdKnownBroken());
  }

  /**
   * Returns a connection<br>
   * The implementation of this method can return a new one or an existing one.
   * The handling of this can delegated to transactional frameworks, like Spring.
   * <pre>
   * Spring eg: Connection conn = DataSourceUtils.getConnection(dataSource);
   * </pre>
   *
   * @return the connection
   */
  public abstract Connection getConnection();

  /**
   * gets the value at the end of the association from the database, puts in the input bean and returns the value.
   * This method relies in reflection
   *
   * @param bean        the target bean
   * @param association the mapping
   * @param <T> type to be returned
   * @return the bean instance returned from de data base
   */
  @SuppressWarnings("unchecked")
  public <T> T loadAssociation(Object bean, Association association) {
    // check if target property is set
    String targetAlias = association.getAlias();
    TypedField targetTf = FieldUtils.getBeanTypedField(bean.getClass(), targetAlias);
    if(targetTf == null) {
      return null;
    }
    Object value;
    try {
      value = targetTf.get(bean);
      if (value != null) { // if value is set, returns
        return (T) value;
      } else if (bean instanceof Updatable) {
        Set<String> changed = ((Updatable) bean).changed();
        if (changed != null && changed.contains(targetTf.getName())) {
          return null;
        }
      }

      Class<?> klass = targetTf.getPropertyType();

      Relation[] relations = association.getRelations();
      List<Condition> restrictions = new ArrayList<>();
      for (Relation relation : relations) {
        // from source FK
        String fkAlias = relation.getFrom().getColumn().getAlias();
        TypedField tf = FieldUtils.getBeanTypedField(bean.getClass(), fkAlias);
        if(tf != null) {
          value = tf.get(bean);
          restrictions.add(relation.getTo().getColumn().is(Definition.raw(value)));
        }
      }

      Query query = query(association.getTableTo()).all()
          .where(restrictions);
      if (Collection.class.isAssignableFrom(klass)) {
        Class<?> genKlass = FieldUtils.getTypeGenericClass(targetTf.getType());
        value = query.list(genKlass);
        if (Set.class.isAssignableFrom(klass)) {
          value = new LinkedHashSet<Object>((Collection<?>) value);
        } else if (List.class.isAssignableFrom(klass)) {
          value = new ArrayList<Object>((Collection<?>) value);
        }
      } else {
        value = query.select(klass);
      }

      targetTf.set(bean, value);

    } catch (Exception ex) {
      throw new PersistenceException("Unable to retrieve association " + association + " for " + bean, ex);
    }

    return (T) value;
  }

  /**
   * selct over a table.<br>
   * If no columns are added, when executing a select or list all columns of the driving table will be added.
   *
   * @param table table to query
   * @return Query
   */
  public Query query(Table table) {
    return new Query(this, table);
  }

  public Query query(Query query) {
    return new Query(query);
  }

  public Driver getDriver() {
    return this.driver;
  }

  public Translator getTranslator() {
    return translator;
  }

  public JdbcSession getJdbcSession() {
    return jdbcSession;
  }

  public Insert insert(Table table) {
    return new Insert(this, table);
  }

  public Update update(Table table) {
    return new Update(this, table);
  }

  public Delete delete(Table table) {
    return new Delete(this, table);
  }
}
