package com.github.quintans.ezSQL.db;

import com.github.quintans.ezSQL.dml.UpdateDSL;

public interface PreUpdateTrigger {
    void trigger(UpdateDSL update);
}
