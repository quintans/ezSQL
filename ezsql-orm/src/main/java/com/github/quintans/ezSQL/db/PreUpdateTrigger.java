package com.github.quintans.ezSQL.db;

import com.github.quintans.ezSQL.dml.Update;

public interface PreUpdateTrigger {
    void trigger(Update update);
}
