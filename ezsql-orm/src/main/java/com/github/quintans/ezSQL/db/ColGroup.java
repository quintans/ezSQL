package com.github.quintans.ezSQL.db;

import java.util.ArrayList;

import com.github.quintans.ezSQL.toolkit.utils.Misc;
import com.github.quintans.jdbc.exceptions.PersistenceException;

public class ColGroup extends ArrayList<Column<?>> {
    private static final long serialVersionUID = 1L;

    public Relashionships TO(Column<?>... to) {
        if (this.size() != Misc.length(to)) {
            throw new PersistenceException("The number of source columns is different from the number of target columns.");
        }
        Relashionships relations = new Relashionships(this.size());
        int k = 0;
        for (Column<?> from : this) {
            relations.add(new Relation(from, to[k]));
            k++;
        }
        return relations;
    }

    public ColGroup(Column<?>... from){
        super(from.length);
        for (Column<?> source : from) {
            this.add(source);
        }            
    }
}
