package com.github.quintans.ezSQL.translator;

import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.dml.Condition;
import com.github.quintans.ezSQL.dml.DeleteDSL;
import com.github.quintans.ezSQL.toolkit.utils.Appender;

public class GenericDeleteBuilder implements DeleteBuilder {
  protected DeleteDSL delete;
  protected Appender tablePart = new Appender(", ");
  protected Appender wherePart = new Appender(" AND ");

  public GenericDeleteBuilder(DeleteDSL delete) {
    this.delete = delete;
    from();
    where();
  }

  protected Translator driver() {
    return this.delete.getTranslator();
  }

  public void from() {
    Table table = delete.getTable();
    String alias = delete.getTableAlias();
    this.tablePart.addAsOne(driver().tableName(table), " ", driver().tableAlias(alias));
  }

  @Override
  public String getTablePart() {
    return this.tablePart.toString();
  }

  public void where() {
    Condition criteria = delete.getCondition();
    if (criteria != null) {
      this.wherePart.add(driver().translate(EDml.DELETE, criteria));
    }
  }

  @Override
  public String getWherePart() {
    return wherePart.toString();
  }

}
