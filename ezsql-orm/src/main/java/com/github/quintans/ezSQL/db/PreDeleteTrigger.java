package com.github.quintans.ezSQL.db;

import com.github.quintans.ezSQL.dml.Delete;

public interface PreDeleteTrigger {
    void trigger(Delete delete);
}
