package com.github.quintans.ezSQL.db;

import com.github.quintans.ezSQL.exception.OrmException;
import com.github.quintans.ezSQL.toolkit.utils.Misc;

import java.util.ArrayList;
import java.util.Collections;

public class ColGroup extends ArrayList<Column<?>> {
  private static final long serialVersionUID = 1L;

  public Relationships TO(Column<?>... to) {
    if (this.size() != Misc.length(to)) {
      throw new OrmException("The number of source columns is different from the number of target columns.");
    }
    Relationships relations = new Relationships(this.size());
    int k = 0;
    for (Column<?> from : this) {
      relations.add(new Relation(from, to[k]));
      k++;
    }
    return relations;
  }

  public ColGroup(Column<?>... from) {
    super(from.length);
    Collections.addAll(this, from);
  }
}
