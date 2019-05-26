package com.github.quintans.ezSQL.repository;

import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.dml.Condition;
import com.github.quintans.ezSQL.dml.Function;
import com.github.quintans.ezSQL.dml.Query;
import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.exceptions.OptimisticLockException;
import com.github.quintans.ezSQL.toolkit.reflection.FieldUtils;
import com.github.quintans.ezSQL.toolkit.reflection.TypedField;
import com.github.quintans.jdbc.exceptions.PersistenceException;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.github.quintans.ezSQL.dml.Definition.count;
import static com.github.quintans.ezSQL.dml.Definition.raw;

public class TypedDb extends com.github.quintans.ezSQL.Db {
    public TypedDb(Driver driver, Connection connection) {
        super(driver, connection);
    }

    public <T, K> T findById(TypedTable<T, K> table, K id) {
        Column<K> keyColumn = table.getSingleKeyColumn();

        return query(table)
                .where(keyColumn.is(id))
                .select(table.getType());
    }

    public <T> List<T> findAll(TypedTable<T, ?> table) {
        return query(table)
                .list(table.getType());
    }

    public <T> Collection<T> find(TypedTable<T, ?> table, Condition... conditions) {
        return query(table).where(conditions).list(table.getType());
    }

    public <T> Collection<T> find(TypedTable<T, ?> table, List<Condition> conditions) {
        return query(table).where(conditions).list(table.getType());
    }

    public <T> T save(TypedTable<T, ?> table, T entity) {

        String alias = table.getVersionColumn().getAlias();
        TypedField tf = FieldUtils.getBeanTypedField(entity.getClass(), alias);
        if (tf != null) {
            Object version;
            try {
                version = tf.get(entity);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new PersistenceException("Unable to get version for " + entity.getClass().getCanonicalName(), e);
            }
            if (version == null) {
                try {
                    tf.set(entity, 1);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new PersistenceException("Unable to set version for " + entity.getClass().getCanonicalName(), e);
                }
            }
        }

        update(table).submit(entity);
        return entity;
    }

    public <T, K> boolean deleteById(TypedTable<T, K> table, K id) {
        Column<?> keyColumn = table.getSingleKeyColumn();

        int result = delete(table)
                .where(
                        keyColumn.is(raw(id))
                )
                .execute();

        return result != 0;
    }

    public <T, K> void deleteByIdAndVersion(TypedTable<T, K> table, K id, Number version) {
        Column<?> keyColumn = table.getSingleKeyColumn();
        Column<?> versionColumn = table.getVersionColumn();

        int result = delete(table)
                .where(
                        keyColumn.is(raw(id)),
                        versionColumn.is(raw(version))
                )
                .execute();

        if (result == 0)
            throw new OptimisticLockException("Unable to delete by id=" + id + " and version=" + version + " for the table " + table.getName());
    }

    public <T> boolean delete(TypedTable<T, ?> table, T entity) {
        return delete(table).execute(entity);
    }

    public <T> PageResults<T> queryForPage(TypedTable<T, ?> table, Page page) {
        return queryForPage(table.getType(), query(table), page);
    }

    public <T> PageResults<T> queryForPage(TypedTable<T, ?> table, Page page, List<Condition> conditions) {
        return queryForPage(table.getType(), query(table).where(conditions), page);
    }

    public <T> PageResults<T> queryForPage(Class<T> type, Query query, Page page) {
        Long skip = (page.getPage() - 1) * page.getPageSize();
        Long max = page.getPageSize();
        query.skip(skip.intValue());

        if (max != null)
            query.limit(max.intValue() + 1);

        Table tab = query.getTable();
        List<T> entities = query.list(type, false);

        PageResults<T> pageResults = new PageResults<>();
        int size = entities.size();
        if (max != null && size > page.getPageSize().intValue()) {
            List<T> res = new ArrayList<>();
            int cnt = 0;
            for (T entity : entities) {
                if (cnt < max)
                    res.add(entity);
                cnt++;
            }
            entities = res;
            pageResults.setLast(false);
        } else {
            pageResults.setLast(true);
        }
        pageResults.setResults(entities);

        // count records
        if (page.getCountRecords()) {
            Query cnt = new Query(this, query.getTable());
            cnt.copy(query);
            List<Function> cols = cnt.getColumns();
            cols.clear();
            cols.add(count());
            if (cnt.getSorts() != null)
                cnt.getSorts().clear();
            long recs = cnt.uniqueLong();
            pageResults.setCount(recs);
        }

        return pageResults;
    }
}
