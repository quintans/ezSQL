package pt.quintans.ezSQL.db;

import pt.quintans.ezSQL.dml.Delete;

public interface PreDeleteTrigger {
    void trigger(Delete delete);
}
