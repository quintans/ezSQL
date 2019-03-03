package com.github.quintans.ezSQL.db;

import com.github.quintans.ezSQL.Base;
import com.github.quintans.ezSQL.dml.ColumnHolder;
import com.github.quintans.ezSQL.dml.Condition;
import com.github.quintans.ezSQL.dml.Definition;
import com.github.quintans.ezSQL.dml.Function;
import com.github.quintans.ezSQL.toolkit.utils.Strings;

/**
 * Definição das colunas a usar numa tabela.
 * ou na obtenção de uma nova(NEW) coluna
 * 
 * @param <T>
 */
public class Column<T> extends Base<T> {

	/**
	 * chave primária
	 */
	/**
	 * referencia para a tabela
	 */
	protected Table table = null;
	/**
	 * nome da coluna
	 */
	protected String name = null;
	/**
	 * construtor
	 */

	protected String alias = null;
	protected NullSql type;

	private boolean key = false;
	private boolean mandatory = false;
	private boolean version = false;
	private boolean deletion = false;

	protected Column() {
	}

	/**
	 * Creates a column of a table
	 * 
	 * @param column
	 *            The name of the column
	 * @param type column type           
	 */
	public Column(String column, NullSql type) {
		this.name = column;
		this.alias = Strings.toCamelCase(column);
		this.type = type;
	}
	
    public NullSql getType() {
        return type;
    }

    /**
	 * @param alias
	 *            The alias of the column
	 * @return
	 */
	@SuppressWarnings("unchecked")
    public Column<T> AS(String alias) {
		this.alias = alias;
		return this;
	}

	public ColumnHolder of(String tableAlias) {
		return new ColumnHolder(this).of(tableAlias);
	}

	/**
	 * set this as a key column
	 * 
	 * @return
	 */
	@SuppressWarnings("unchecked")
    public Column<T> key() {
		this.key = true;
		this.table.addKey(this);
		return this;
	}

	/**
	 * set this as a mandatory column
	 * 
	 * @return
	 */
	public Column<T> mandatory() {
		this.mandatory = true;
		return this;
	}

	/**
	 * set this as a version column
	 * 
	 * @return
	 */
	@SuppressWarnings("unchecked")
    public Column<T> version() {
		this.version = true;
		this.table.setVersionColumn(this);
		return this;
	}

	/**
	 * set this as a deletion column
	 * 
	 * @return
	 */
	@SuppressWarnings("unchecked")
    public Column<T> deletion() {
		this.deletion = true;
		this.table.setDeletionColumn(this);
		return this;
	}

	public Column<T> WITH(T... values) {
	    this.getTable().WITH(this, values);
	    return this;
	}

	/**
	 * obtem a referencia para a tabela a que pertence esta coluna
	 * 
	 * @return a tabela
	 */
	public Table getTable() {
		return this.table;
	}

	public String getAlias() {
		return this.alias;
	}

	/**
	 * obtem o nome da coluna
	 * 
	 * @return nome da coluna
	 */
	public String getName() {
		return this.name;
	}

    /**
	 * indica se é uma coluna chave
	 * 
	 * @return se é coluna chave
	 */
	public boolean isKey() {
		return this.key;
	}

	public boolean isMandatory() {
		return this.mandatory;
	}

	public boolean isVersion() {
		return this.version;
	}

	public boolean isDeletion() {
		return this.deletion;
	}
	
	public Condition is() {
        return is(param());
	}
	
    public Function param() {
        return Definition.param(this);
    }
    
	/**
	 * devolve a representação em String desta coluna.
	 * 
	 * @return devolve string com o formato 'table.coluna'
	 */
	@Override
	public String toString() {
		return this.table + "." + this.name;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;

		if (o == null)
			return false;

		if (o instanceof Column) {
			Column<?> col = (Column<?>) o;
			return col.table.equals(this.table) && this.name.equals(col.name);
		}

		return false;
	}

	private int _hashCode = 0;

	@Override
	public int hashCode() {
		if (this._hashCode == 0)
			this._hashCode = (this.table + "." + this.name).hashCode();

		return this._hashCode;
	}
}
