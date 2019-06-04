package com.github.quintans.ezSQL.db;

import com.github.quintans.ezSQL.dml.Condition;
import com.github.quintans.ezSQL.dml.Definition;
import com.github.quintans.ezSQL.dml.Function;

public class Discriminator {
  private Column<?> column;
  private Function value;
  private Condition condition;

  public Discriminator(Column<?> column, Object value) {
    this.column = column;
    if (value == null) {
      value = column.getType();
    }
    this.value = Function.converteOne(value);
    this.condition = Definition.eq(this.column, this.value);
  }

  public Column<?> getColumn() {
    return this.column;
  }

  public Function getValue() {
    return this.value;
  }

  public Condition getCondition() {
    return this.condition;
  }

}
