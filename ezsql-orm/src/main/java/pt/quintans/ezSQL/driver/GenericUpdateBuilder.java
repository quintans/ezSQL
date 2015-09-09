package pt.quintans.ezSQL.driver;

import java.util.Map;
import java.util.Map.Entry;

import pt.quintans.ezSQL.db.Column;
import pt.quintans.ezSQL.db.Table;
import pt.quintans.ezSQL.dml.Condition;
import pt.quintans.ezSQL.dml.Function;
import pt.quintans.ezSQL.dml.Update;
import pt.quintans.ezSQL.toolkit.utils.Appender;

public class GenericUpdateBuilder implements UpdateBuilder {
    protected Update update;
    protected Appender tablePart = new Appender();
    protected Appender columnPart = new Appender(", ");
    protected Appender wherePart = new Appender(" AND ");
    
    public GenericUpdateBuilder(Update update) {
        this.update = update;
        columns();
        from();
        where();
    }
    
    protected Driver driver() {
        return this.update.getDb().getDriver();
    }
    
    @Override
    public String getColumnPart() {
        return this.columnPart.toString();
    }

    @Override
    public String getTablePart() {
        return this.tablePart.toString();
    }

    @Override
    public String getWherePart() {
        return this.wherePart.toString();
    }

    public void columns() {
        Map<Column<?>, Function> values = update.getValues();
        if(values != null) {        
            for (Entry<Column<?>, Function> entry : values.entrySet()) {
                column(entry.getKey(), entry.getValue());
            }
        }
    }
    
    public void column(Column<?> column, Function token){
        this.columnPart.addAsOne(update.getTableAlias(), ".",
                driver().columnName(column),
            " = ", driver().translate(EDml.UPDATE, token));
    }

    public void from() {
        Table table = update.getTable();
        String alias = update.getTableAlias();
        this.tablePart.addAsOne(driver().tableName(table), " ", driver().tableAlias(alias));
    }

    public void where() {
        Condition criteria = update.getCondition();
        if (criteria != null) {
            this.wherePart.add(driver().translate(EDml.UPDATE, criteria));
        }
    }

}
