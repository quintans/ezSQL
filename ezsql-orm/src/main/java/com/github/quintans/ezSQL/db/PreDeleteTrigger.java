package com.github.quintans.ezSQL.db;

import com.github.quintans.ezSQL.dml.DeleteDSL;

public interface PreDeleteTrigger {
    void trigger(DeleteDSL delete);
}
