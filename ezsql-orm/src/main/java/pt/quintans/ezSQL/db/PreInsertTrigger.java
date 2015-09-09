package pt.quintans.ezSQL.db;

import pt.quintans.ezSQL.dml.Insert;

public interface PreInsertTrigger {
    void trigger(Insert insert);
}
