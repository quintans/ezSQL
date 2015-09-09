package pt.quintans.ezSQL.driver;

import java.util.Map;
import java.util.Map.Entry;

import pt.quintans.ezSQL.db.Column;
import pt.quintans.ezSQL.db.NullSql;
import pt.quintans.ezSQL.db.Table;
import pt.quintans.ezSQL.dml.EFunction;
import pt.quintans.ezSQL.dml.Function;
import pt.quintans.ezSQL.dml.Insert;
import pt.quintans.ezSQL.toolkit.utils.Appender;

public class GenericInsertBuilder implements InsertBuilder {
    protected Insert insert;
    protected Appender tablePart = new Appender();
    protected Appender columnPart = new Appender(", ");
    protected Appender valuePart = new Appender(", ");
    
    public GenericInsertBuilder(Insert insert) {
        this.insert = insert;
        from();
        column();
    }
    
    protected Driver driver() {
        return this.insert.getDb().getDriver();
    }
    
    @Override
    public String getColumnPart() {
        return this.columnPart.toString();
    }

    @Override
    public String getValuePart() {
        return this.valuePart.toString();
    }

    @Override
    public String getTablePart() {
        return this.tablePart.toString();
    }

    public void column() {
        Map<Column<?>, Function> values = null;
        values = insert.getValues();
        Map<String, Object> parameters = insert.getParameters();
        if(values != null) {
            for (Entry<Column<?>, Function> entry : values.entrySet()) {
                Column<?> column = entry.getKey();
                Function token = entry.getValue();
                // only includes null keys if IgnoreNullKeys is false
                if (column.isKey() && driver().ignoreNullKeys() && EFunction.PARAM == token.getOperator()) {
                    // ignore null keys
                    Object param = parameters.get(token.getValue());
                    if (param == null || param instanceof NullSql) {
                        token = null;
                    }
                }

                if(token != null) {
                    String val = driver().translate(EDml.INSERT, token);
                    if (val != null) {
                        this.columnPart.add(driver().columnName(column));
                        this.valuePart.add(val);
                    }
                }
            }
        }
    }

    public void from()  {
        Table table = insert.getTable();
        this.tablePart.add(driver().tableName(table));
    }
    

}
