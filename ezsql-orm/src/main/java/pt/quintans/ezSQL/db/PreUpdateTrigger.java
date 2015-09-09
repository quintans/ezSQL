package pt.quintans.ezSQL.db;

import pt.quintans.ezSQL.dml.Update;

public interface PreUpdateTrigger {
    void trigger(Update update);
}
