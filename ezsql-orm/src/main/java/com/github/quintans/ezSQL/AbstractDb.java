package com.github.quintans.ezSQL;

import com.github.quintans.ezSQL.common.api.Converter;
import com.github.quintans.ezSQL.common.api.Updatable;
import com.github.quintans.ezSQL.common.type.MyDate;
import com.github.quintans.ezSQL.common.type.MyDateTime;
import com.github.quintans.ezSQL.common.type.MyTime;
import com.github.quintans.ezSQL.db.*;
import com.github.quintans.ezSQL.dml.*;
import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.toolkit.io.BinStore;
import com.github.quintans.ezSQL.toolkit.io.TextStore;
import com.github.quintans.ezSQL.toolkit.reflection.FieldUtils;
import com.github.quintans.ezSQL.toolkit.reflection.TypedField;
import com.github.quintans.ezSQL.transformers.*;
import com.github.quintans.jdbc.JdbcSession;
import com.github.quintans.jdbc.SimpleJdbc;
import com.github.quintans.jdbc.exceptions.PersistenceException;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public abstract class AbstractDb {
    private static Logger LOGGER = Logger.getLogger(AbstractDb.class);

    private Driver driver;
    private JdbcSession jdbcSession;

    private Collection<QueryMapper> queryMappers;
    private Collection<InsertMapper> insertMappers;
    private Collection<DeleteMapper> deleteMappers;
    private Collection<UpdateMapper> updateMappers;
    private ConcurrentHashMap<Class<? extends Converter>, Converter> converters;

    public AbstractDb(Driver driver) {
        this.driver = driver;
        this.jdbcSession = new DbJdbcSession(this, driver.isPmdKnownBroken());
        this.queryMappers = new ConcurrentLinkedDeque<>();
        this.queryMappers.add(new QueryMapperBean());
        this.insertMappers  = new ConcurrentLinkedDeque<>();
        this.insertMappers.add(new InsertMapperBean());
        this.deleteMappers = new ConcurrentLinkedDeque<>();
        this.deleteMappers.add(new DeleteMapperBean());
        this.updateMappers = new ConcurrentLinkedDeque<>();
        this.updateMappers.add(new UpdateMapperBean());
        this.converters = new ConcurrentHashMap<>();
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
     * @return the bean instance returned from de data base
     */
    @SuppressWarnings("unchecked")
    public <T> T loadAssociation(Object bean, Association association) {
        // check if target property is set
        String targetAlias = association.getAlias();
        TypedField targetTf = FieldUtils.getBeanTypedField(bean.getClass(), targetAlias);
        Object value;
        try {
            value = targetTf.get(bean);
            if (value != null) { // if value is set returns
                return (T) value;
            } else if (bean instanceof Updatable) {
                Set<String> changed = ((Updatable) bean).changed();
                if (changed != null && changed.contains(targetTf.getName())) {
                    return null;
                }
            }

            Class<?> klass = targetTf.getPropertyType();

            Relation[] relations = association.getRelations();
            List<Condition> restrictions = new ArrayList<Condition>();
            for (Relation relation : relations) {
                // from source FK
                String fkAlias = relation.getFrom().getColumn().getAlias();
                TypedField tf = FieldUtils.getBeanTypedField(bean.getClass(), fkAlias);
                value = tf.get(bean);
                restrictions.add(relation.getTo().getColumn().is(Definition.raw(value)));
            }

            Query query = query(association.getTableTo()).all()
                    .where(restrictions);
            if (Collection.class.isAssignableFrom(klass)) {
                Class<?> genKlass = FieldUtils.getTypeGenericClass(targetTf.getType());
                value = query.list(genKlass);
                if (Set.class.isAssignableFrom(klass)) {
                    value = new LinkedHashSet<Object>((Collection<? extends Object>) value);
                } else if (List.class.isAssignableFrom(klass)) {
                    value = new ArrayList<Object>((Collection<? extends Object>) value);
                }
            } else {
                value = query.select(klass);
            }

            targetTf.set(bean, value);

        } catch (Exception ex) {
            throw new PersistenceException("Unable to retrive association " + association + " for " + bean, ex);
        }

        return (T) value;
    }

    /**
     * selct over a table.<br>
     * If no columns are added, when executing a select or list all columns of the driving table will be added.
     *
     * @param table
     * @return
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

    public JdbcSession getJdbcSession() {
        return jdbcSession;
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
        SimpleJdbc jdbc = new SimpleJdbc(jdbcSession);
        Long id = jdbc.queryForLong(sql, new LinkedHashMap<String, Object>());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("executed in: " + (System.nanoTime() - now) / 1e6 + "ms");
        }
        return id;
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

    public final void registerQueryMappers(QueryMapper... mappers) {
        registerMappers(this.queryMappers, mappers);
    }

    public QueryMapper findQueryMapper(Class<?> klass) {
        return findMapper(queryMappers, klass, QueryMapper.class.getSimpleName());
    }

    public final void registerInsertMappers(InsertMapper... mappers) {
        registerMappers(this.insertMappers, mappers);
    }

    public InsertMapper findInsertMapper(Class<?> klass) {
        return findMapper(insertMappers, klass, InsertMapper.class.getSimpleName());
    }

    public final void registerDeleteMappers(DeleteMapper... mappers) {
        registerMappers(this.deleteMappers, mappers);
    }

    public DeleteMapper findDeleteMapper(Class<?> klass) {
        return findMapper(deleteMappers, klass, DeleteMapper.class.getSimpleName());
    }

    public final void registerUpdateMappers(UpdateMapper... mappers) {
        registerMappers(this.updateMappers, mappers);
    }

    public UpdateMapper findUpdateMapper(Class<?> klass) {
        return findMapper(updateMappers, klass, UpdateMapper.class.getSimpleName());
    }

    public Converter getConverter(Class<? extends Converter> converter) {
        return converters.computeIfAbsent(converter, aClass -> {
            try {
                return aClass.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                throw new PersistenceException(e);
            }
        });
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
}
