package pt.quintans.ezSQL.driver;

import pt.quintans.ezSQL.db.Table;
import pt.quintans.ezSQL.dml.Condition;
import pt.quintans.ezSQL.dml.Delete;
import pt.quintans.ezSQL.toolkit.utils.Appender;

public class GenericDeleteBuilder implements DeleteBuilder {
    protected Delete delete;
    protected Appender tablePart = new Appender(", ");
    protected Appender wherePart = new Appender(" AND ");
    
    public GenericDeleteBuilder(Delete delete) {
        this.delete = delete;
        from();
        where();
    }

    protected Driver driver() {
        return this.delete.getDb().getDriver();
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
