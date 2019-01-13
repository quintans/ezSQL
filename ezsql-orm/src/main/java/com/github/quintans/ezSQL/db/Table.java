package com.github.quintans.ezSQL.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.github.quintans.ezSQL.common.type.MyDate;
import com.github.quintans.ezSQL.common.type.MyTime;
import com.github.quintans.ezSQL.dml.Condition;
import com.github.quintans.ezSQL.toolkit.io.BinStore;
import com.github.quintans.ezSQL.toolkit.io.TextStore;
import com.github.quintans.ezSQL.toolkit.utils.Misc;
import com.github.quintans.jdbc.exceptions.PersistenceException;

public class Table {
    private Map<String, Column<?>> columnsMap = new LinkedHashMap<String, Column<?>>();
    private Map<String, Association> associationMap = null;

    /**
     * table name
     */
    protected String name = null;
    /**
     * table alias
     */
    protected String alias = null;
    /**
     * lista das colunas
     */
    protected Set<Column<?>> columns = new LinkedHashSet<Column<?>>();
    protected Column<?> singleKey;
    protected Set<Column<?>> keys = new LinkedHashSet<Column<?>>();
    protected Column<?> version = null;
    protected Column<?> deletion = null;

    protected List<Discriminator> discriminators;

    protected PreInsertTrigger preInsertTrigger;
    protected PreUpdateTrigger preUpdateTrigger;
    protected PreDeleteTrigger preDeleteTrigger;
    
    /**
     * contrutor
     * 
     * @param table
     *            nome da tabela
     */
    public Table(String table) {
        if (table == null || "".equals(table))
            throw new NullPointerException("Null for table is not allowed.");
        this.name = table;
        this.alias = Misc.toCamelCase(table);
    }
    
    public Table copy(){
        Table table = new Table(this.name);
        table.alias = this.alias;

        table.columnsMap = new LinkedHashMap<String, Column<?>>(this.columnsMap);
        if(this.associationMap != null) {
            table.associationMap = new LinkedHashMap<String, Association>(this.associationMap);
        }

        /**
         * lista das colunas
         */
        table.columns = new LinkedHashSet<Column<?>>(this.columns);
        table.singleKey = singleKey;
        table.keys = new LinkedHashSet<Column<?>>(this.keys);
        table.version = this.version;
        table.deletion = this.deletion;

        if(table.discriminators != null) {
            table.discriminators = new ArrayList<Discriminator>(this.discriminators);
        }

        table.preInsertTrigger = this.preInsertTrigger;
        table.preUpdateTrigger = this.preUpdateTrigger;
        table.preDeleteTrigger = this.preDeleteTrigger;
        
        return table;
    }
    
    public PreInsertTrigger getPreInsertTrigger() {
        return preInsertTrigger;
    }

    public void setPreInsertTrigger(PreInsertTrigger preInsertTrigger) {
        this.preInsertTrigger = preInsertTrigger;
    }

    public PreUpdateTrigger getPreUpdateTrigger() {
        return preUpdateTrigger;
    }

    public void setPreUpdateTrigger(PreUpdateTrigger preUpdateTrigger) {
        this.preUpdateTrigger = preUpdateTrigger;
    }

    public PreDeleteTrigger getPreDeleteTrigger() {
        return preDeleteTrigger;
    }

    public void setPreDeleteTrigger(PreDeleteTrigger preDeleteTrigger) {
        this.preDeleteTrigger = preDeleteTrigger;
    }

    public String getAlias() {
        return this.alias;
    }

    public Table AS(String alias) {
        this.alias = alias;
        return this;
    }

    public Column<Boolean> BOOLEAN(String name){
        return COLUMN(name, NullSql.BOOLEAN);
    }
    
    public Column<Character> CHAR(String name){
        return COLUMN(name, NullSql.CHAR);
    }
    
    public <T> Column<T> NUMBERED(String name){
        return COLUMN(name, NullSql.INTEGER);
    }
    
    public <T> Column<T> NAMED(String name){
        return COLUMN(name, NullSql.VARCHAR);
    }
    
    public Column<String> VARCHAR(String name){
        return COLUMN(name, NullSql.VARCHAR);
    }
    
    public Column<Integer> TINY(String name){
        return COLUMN(name, NullSql.TINY);
    }
    
    public Column<Integer> SMALL(String name){
        return COLUMN(name, NullSql.SMALL);
    }
    
    public Column<Integer> INTEGER(String name){
        return COLUMN(name, NullSql.INTEGER);
    }
    
    public Column<Long> BIGINT(String name){
        return COLUMN(name, NullSql.BIGINT);
    }
        
    public Column<Double> DECIMAL(String name){
        return COLUMN(name, NullSql.DECIMAL);
    }
        
    public Column<MyTime> TIME(String name){
        return COLUMN(name, NullSql.TIME);
    }
    
    public Column<MyDate> DATE(String name){
        return COLUMN(name, NullSql.DATE);
    }
    
    public Column<Date> DATETIME(String name){
        return COLUMN(name, NullSql.DATETIME);
    }
    
    public Column<Date> TIMESTAMP(String name){
        return COLUMN(name, NullSql.TIMESTAMP);
    }
    
    public <T> Column<T> CLOB(String name){
        return COLUMN(name, NullSql.CLOB);
    }
    
    public Column<byte[]> BLOB(String name){
        return COLUMN(name, NullSql.BLOB);
    }
    
    public Column<BinStore> BIN(String name){
        return COLUMN(name, NullSql.BLOB);
    }
    
    public Column<TextStore> TEXT(String name){
        return COLUMN(name, NullSql.CLOB);
    }
    
    public <T> Column<T> COLUMN(String name, NullSql type) {
        Column<T> col = new Column<T>(name, type);
        addColumn(col);
        return col;
    }
    
    private void addColumn(Column<?> col) {
        col.table = this;
        if (!this.columns.contains(col)) {
            this.columns.add(col);

            // checks if this column alias uniqueness
            if (this.columnsMap.get(col.getAlias()) != null) {
                throw new PersistenceException(String.format("The alias '%s' for the column '%s' is not unique!", col.getAlias(), col.toString()));
            } else {
                this.columnsMap.put(col.getAlias(), col);
            }
        }
    }  

    /**
     * Descriminator. Restriction applied to the table enabling several domains to share the same table.
     * 
     * @param column
     * @param values
     * @return
     */
    public <T> Table WITH(Column<T> column, T... values) {
        if (this.discriminators == null) {
            this.discriminators = new ArrayList<Discriminator>();
        }
        for(Object value : values) {
            this.discriminators.add(new Discriminator(column, value));
        }
        return this;
    }
    
    public ColGroup ASSOCIATE(Column<?>... from) {
        // all columns must be from this table.
        for (Column<?> source : from) {
            if (!this.equals(source.getTable())) {
                throw new PersistenceException(source.toString() + " does not belong to " + this.toString());
            }
        }

        return new ColGroup(from);
    }

    /**
     * obtem o nome da tabela
     * 
     * @return nome da tabela
     */
    public String getName() {
        return this.name;
    }

    /**
     * obtem lista das tabelas
     * 
     * @return lista das tabelas
     */
    public Set<Column<?>> getColumns() {
        return this.columns;
    }

    public List<Column<?>> getBasicColumns() {
        ArrayList<Column<?>> list = new ArrayList<Column<?>>();
        for (Column<?> column : this.columns) {
            if (!column.isKey() && !column.isVersion() && !column.isDeletion())
                list.add(column);
        }
        return list;
    }

    /**
     * devolve a representação da tabela em String
     * 
     * @return a tabela em String
     */
    @Override
    public String toString() {
        return this.name;
    }

    public Column<?> getSingleKeyColumn() {
        return singleKey;
    }

    public Set<Column<?>> getKeyColumns() {
        return this.keys;
    }
    
    protected void addKey(Column<?> col) {
        this.keys.add(col);
        if (this.keys.size() == 1) {
            this.singleKey = col;
        } else {
            // it is only allowed one single key column
            this.singleKey = null;
        }
    }    

    public Column<?> getVersionColumn() {
        return this.version;
    }

    protected void setVersionColumn(Column<?> column) {
        this.version = column;
    }

    public Column<?> getDeletionColumn() {
        return this.deletion;
    }

    protected void setDeletionColumn(Column<?> column) {
        this.deletion = column;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null)
            return false;

        if (obj instanceof Table) {
            Table table = (Table) obj;
            return this.alias.equals(table.alias) &&
                    this.name.equals(table.name);
        }

        return false;
    }

    public Association addAssociation(Association fk) {
        return addAssociation(fk.getAlias(), fk);
    }

    public Association addAssociation(String name, Association fk) {
        if (this.associationMap == null) {
            this.associationMap = new LinkedHashMap<String, Association>();
        } else {
            if (this.associationMap.containsKey(name)) {
                throw new PersistenceException(
                        String.format("A associação %s já se encontra mapeada para a tabela %s com o valor %s",
                                fk.toString(), getAlias(), this.associationMap.get(fk.getAlias()).toString()));
            }
        }

        this.associationMap.put(name, fk);
        return fk;
    }

    public Collection<Association> getAssociations() {
        if (this.associationMap != null)
            return this.associationMap.values();
        else
            return null;
    }

    public List<Discriminator> getDiscriminators() {
        return this.discriminators;
    }

    public List<Condition> getConditions() {
        if(discriminators != null){
            List<Condition> conditions = new ArrayList<Condition>(discriminators.size());
            for(Discriminator disc : discriminators){
                conditions.add(disc.getCondition());
            }
            return conditions;
        } else {
            return null;
        }
    }

}
