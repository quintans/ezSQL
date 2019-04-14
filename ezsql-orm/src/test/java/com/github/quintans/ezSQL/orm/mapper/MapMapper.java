package com.github.quintans.ezSQL.orm.mapper;

import com.github.quintans.ezSQL.transformers.MapColumn;
import com.github.quintans.ezSQL.transformers.QueryMapper;
import com.github.quintans.ezSQL.transformers.Record;
import com.github.quintans.jdbc.exceptions.PersistenceException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Very naive implementation of a mapper
 *
 * @param <T>
 */
public class MapMapper<T> implements QueryMapper<T> {
    @Override
    public Object createFrom(Object parentInstance, String name) {
        // handling root table
        if (parentInstance == null) {
            return new HashMap<String, Object>();
        }

        throw new PersistenceException("This mapper does not support children");
    }

    @Override
    public void link(Object parentInstance, String name, Object value) {
        Map<String, Object> map = (Map<String, Object>) parentInstance;
        map.put(name, value);
    }

    @Override
    public boolean map(Record record, Object instance, List<MapColumn> mapColumns) {
        for (MapColumn mapColumn : mapColumns) {
            Map<String, Object> map = (Map<String, Object>) instance;
            Object value = record.getObject(mapColumn.getIndex());
            map.put(mapColumn.getAlias(), value);
        }

        return true;
    }

}
