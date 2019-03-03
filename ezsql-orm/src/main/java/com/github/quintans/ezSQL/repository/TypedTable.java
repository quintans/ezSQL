package com.github.quintans.ezSQL.repository;

import com.github.quintans.ezSQL.db.Table;

public class TypedTable<T, K> extends Table {
    private Class<T> type;
    private Class<K> keyType;

    public TypedTable(String table, Class<T> type, Class<K> keyType) {
        super(table);
        this.type = type;
        this.keyType = keyType;
    }

    public Class<T> getType() {
        return type;
    }

    public Class<K> getKeyType() {
        return keyType;
    }
}
