package pt.quintans.ezSQL.db;

import java.util.ArrayList;
import java.util.List;

import pt.quintans.ezSQL.exceptions.PersistenceException;

public class Association {
	protected Table tableMany2Many;

	protected Association fromM2M;
	protected Association toM2M;

	protected Table tableFrom;
	protected Table tableTo;
	protected Relation relations[];

	protected String alias;

	protected String aliasFrom;
	protected String aliasTo;

	private Table discriminatorTable = null;
	private List<Discriminator> discriminators;

	/**
	 * Copy constructor
	 * 
	 * @param fk
	 */
	public Association(Association fk) {
		if (fk.isMany2Many()) {
			defineAssociation(false, fk.getAlias(), fk.getFromM2M().bareCopy(), fk.getToM2M().bareCopy());
		} else {
			Relation relations[] = fk.getRelations();
			Relation rels[] = new Relation[relations.length];
			for (int f = 0; f < rels.length; f++)
				rels[f] = relations[f].bareCopy();
			defineAssociation(false, fk.getAlias(), rels);
			this.discriminators = fk.discriminators;
		}
	}

	public String genericPath() {
		return String.format("%s (%s->%s)", this.alias, this.tableFrom.toString(), this.tableTo.toString());
	}

	public String path() {
		return String.format("%s (%s.%s->%s.%s)", this.alias, this.aliasFrom, this.tableFrom.toString(), this.aliasTo, this.tableTo.toString());
	}

	@Override
	public String toString() {
		return path();
	}

	public Association bareCopy() {
		return new Association(this);
	}

	public boolean isMany2Many() {
		return this.tableMany2Many != null;
	}
	
	public boolean isMany2One() {
		if(!isMany2Many()) {
			return relations[0].getFrom().equals(tableFrom);
		}
		return false;
	}

	public boolean isOne2Many() {
		if(!isMany2Many()) {
			return !relations[0].getFrom().equals(tableFrom);			
		}
		return false;
	}

	// Many To Many
	public Association(String alias, Relashionships relFrom, Relashionships relTo) {
	    Association fkFrom = new Association(relFrom.toArray(new Relation[relFrom.size()]));
        Association fkTo = new Association(relTo.toArray(new Relation[relTo.size()]));
		defineAssociation(true, alias, fkFrom, fkTo);
	}

	private void defineAssociation(boolean associate, String alias, Association fkFrom, Association fkTo) {
		this.alias = alias;

		this.tableMany2Many = fkFrom.getTableFrom();

		this.fromM2M = fkFrom;
		this.tableFrom = this.fromM2M.getTableFrom();
		this.toM2M = fkTo;
		this.tableTo = this.toM2M.getTableTo();

		if (associate) {
			// informo as tabelas desta associação - 02/08/2010
			this.tableFrom.addAssociation(this);
			// tableTo.addAssociation(this); // Quintans: 3/3/2011
		}
	}

	public Association getFromM2M() {
		return this.fromM2M;
	}

	public void setFromM2M(Association fromM2M) {
		this.fromM2M = fromM2M;
	}

	public Association getToM2M() {
		return this.toM2M;
	}

	public void setToM2M(Association toM2M) {
		this.toM2M = toM2M;
	}

	public Table getTableMany2Many() {
		return this.tableMany2Many;
	}

	/**
	 * Cria uma ForeignKey com as relações invertidas da ForeignKey passada<br>
	 * <b>WARNING:<b> this can lead to <code>ExceptionInInitialitionError</code> due to circular references.
	 * 
	 * @param alias
	 * @param fkey
	 */
	public Association(String alias, Association fkey) {
		if (fkey.isMany2Many()) {
			defineAssociation(true, alias, fkey.getToM2M(), fkey.getFromM2M());
		} else {
			// inverte as relações
			Relation[] relacoes = fkey.getRelations();
			Relation rels[] = new Relation[relacoes.length];
			for (int i = 0; i < relacoes.length; i++) {
				rels[i] = new Relation(relacoes[i].getTo().getColumn(), relacoes[i].getFrom().getColumn());
			}
			defineAssociation(true, alias, rels);
			this.discriminators = fkey.discriminators;
		}
	}

	public Association(Relation... relations) {
		defineAssociation(false, null, relations);
	}

	public Association(String alias, Relation... relations) {
		defineAssociation(true, alias, relations);
	}

	public Association WITH(Column<?> column, Object value) {
		if (this.discriminators == null) {
			this.discriminators = new ArrayList<Discriminator>();
		}

		if (this.discriminatorTable != null && !this.discriminatorTable.equals(column.getTable()))
			throw new PersistenceException("Discriminator columns must belong to the same table." + column + " does not belong to " + this.discriminatorTable);

		this.discriminatorTable = column.getTable();
		Discriminator discriminator = new Discriminator(column, value);
		this.discriminators.add(discriminator);
		return this;
	}

	private void defineAssociation(boolean add2Table, String alias, Relation... relations) {
		this.alias = alias;

		Table tableFrom = relations[0].getFrom().getColumn().getTable();
		Table tableTo = relations[0].getTo().getColumn().getTable();
		// super(table);
		// check consistency
		for (Relation relation : relations) {
			if (!tableFrom.equals(relation.getFrom().getColumn().getTable()))
				throw new PersistenceException("left side of " + relation.toString() + " does not belong to " + tableFrom.toString());
			else if (!tableTo.equals(relation.getTo().getColumn().getTable()))
				throw new PersistenceException("right side of " + relation.toString() + " does not belong to " + tableTo.toString());
		}
		this.tableFrom = tableFrom;
		this.tableTo = tableTo;
		this.relations = relations;

		if (add2Table) {
			tableFrom.addAssociation(this);
			// tableTo.addAssociation(this); // Quintans: 5/1/2011
		}
	}

	public String getAlias() {
		return this.alias;
	}

	public Association AS(String alias) {
		this.alias = alias;
		return this;
	}

	public String getAliasFrom() {
		return this.aliasFrom;
	}

	public void setAliasFrom(String aliasFrom) {
		if (this.aliasFrom == null)
			this.aliasFrom = aliasFrom;
	}

	public String getAliasTo() {
		return this.aliasTo;
	}

	public void setAliasTo(String aliasTo) {
		if (this.aliasTo == null)
			this.aliasTo = aliasTo;
	}

	public Table getTableFrom() {
		return this.tableFrom;
	}

	public Table getTableTo() {
		return this.tableTo;
	}

	public Relation[] getRelations() {
		return this.relations;
	}

	public Table getDiscriminatorTable() {
		return this.discriminatorTable;
	}

	public List<Discriminator> getDiscriminators() {
		return this.discriminators;
	}

	public void setDiscriminators(List<Discriminator> discriminators) {
		this.discriminators = discriminators;
	}
}
