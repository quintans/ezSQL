package com.github.quintans.ezSQL.orm.mapper;

import com.github.quintans.ezSQL.mapper.MapColumn;
import com.github.quintans.ezSQL.mapper.QueryMapper;
import com.github.quintans.jdbc.exceptions.PersistenceException;
import com.github.quintans.ezSQL.mapper.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Very naive implementation of a mapper
 */
public class MapMapper implements QueryMapper {
    @Override
    public boolean support(Class<?> rootClass) {
        return Map.class.isAssignableFrom(rootClass);
    }

    @Override
    public Object createRoot(Class<?> rootClass) {
        return new HashMap<String, Object>();
    }

    @Override
    public Object createFrom(Object parentInstance, String name) {
        throw new PersistenceException("This mapper does not support children");
    }

    @Override
    public void link(Object parentInstance, String name, Object value) {
        Map<String, Object> map = (Map<String, Object>) parentInstance;
        map.put(name, value);
    }

    @Override
    public boolean map(Row row, Object instance, List<MapColumn> mapColumns) {
        for (MapColumn mapColumn : mapColumns) {
            Map<String, Object> map = (Map<String, Object>) instance;
            Object value = row.get(mapColumn.getIndex());
            map.put(mapColumn.getAlias(), value);
        }

        return true;
    }

}
