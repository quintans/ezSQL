package com.github.quintans.ezSQL.transformers;

import com.github.quintans.ezSQL.dml.Query;
import com.github.quintans.ezSQL.mapper.Mapper;
import com.github.quintans.ezSQL.mapper.QueryMapper;
import com.github.quintans.jdbc.transformers.IResultTransformer;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;

import java.sql.SQLException;
import java.util.Collection;

public class MapTransformer<T> extends Mapper<T> implements IResultTransformer<T> {

    public MapTransformer(Query query, boolean reuse, Class<T> rootClass, QueryMapper mapper) {
        super(query, reuse, rootClass, mapper);
    }

    public T transform(ResultSetWrapper rsw) throws SQLException {
        return map(new Record(getQuery(), rsw));
    }

    @Override
    public void collect(ResultSetWrapper rsw) throws SQLException{
        super.collect(transform(rsw));
    }

    @Override
    public Collection<T> collection() {
        return super.collection();
    }

}
