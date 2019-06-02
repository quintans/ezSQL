package com.github.quintans.ezSQL.db;

import com.github.quintans.ezSQL.dml.InsertDSL;

public interface PreInsertTrigger {
    void trigger(InsertDSL insert);
}
