package com.github.quintans.ezSQL.db;

import com.github.quintans.ezSQL.dml.Insert;

public interface PreInsertTrigger {
    void trigger(Insert insert);
}
