package com.github.quintans.ezSQL.transformers;

import java.util.ArrayList;
import java.util.List;

import com.github.quintans.ezSQL.db.Association;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.dml.ColumnHolder;
import com.github.quintans.ezSQL.dml.Function;
import com.github.quintans.ezSQL.dml.Join;
import com.github.quintans.ezSQL.dml.PathElement;
import com.github.quintans.ezSQL.dml.Query;
import com.github.quintans.ezSQL.toolkit.utils.Holder;
import com.github.quintans.jdbc.exceptions.PersistenceException;

/**
 * Esta classe serve para manter o estado, enquando se navega pelas associaçõeses e se constroi a arvore de entidades durante, após uma query select.
 * Assim é possivel indicar aos RowTransformers quais os caminhos a percorrer
 * conforme se for navegando pelas associacoes, a lista de navegações possiveis vai variando.
 * 
 * Quintans - 10.01.2011
 * O processo é feito em 2as fases.
 * 1) construção da arvore (foreign keys comuns são convertidos numa só)
 * para identificar-mos quais as derivações (branches) existentes para um nó
 * 2) construção de uma array que contem somente os nós que contornam a arvore,
 * para ser mais facil o cálculo dos offsets das colunas de cada entidade a processar
 * 
 * @author Quintans 04/06/2010
 * 
 */
public class Navigation {

	private int depth = 0;
	private NavigationNode[] nodes;
	private NavigationNode firstNode;

	public List<NavigationNode> getBranches() {
		if (this.nodes == null || this.depth >= this.nodes.length)
			return null;
		else
			return this.nodes[this.depth].getBranches();
	}

	public void dispose() {
		this.depth = 0;
		this.nodes = null;
		this.firstNode = null;
	}

	private static final NavigationNode[] toArrayMappingTransactionNode = new NavigationNode[0];

	public void prepare(Query query, boolean reuse) {
		Table table = query.getTable();
		checkKeys(table, query.getColumns(), reuse);
		List<Association[]> includes = new ArrayList<Association[]>();
		if (query.getJoins() != null) {
			for (Join join : query.getJoins()) {
				if (join.isFetch()) {
	                for(PathElement pe : join.getPathElements()){
	                    checkKeys(pe.getBase().getTableTo(), pe.getColumns(), reuse);
	                }
				    includes.add(join.getAssociations());
				}
			}
		}

		// reset
		this.depth = 0;
		this.firstNode = new NavigationNode();
		Holder<Table> holder = new Holder<Table>(table); // contem a ultima tabela
		for (Association[] fks : includes)
			this.firstNode.buildTree(fks, holder);

		List<NavigationNode> flat = new ArrayList<NavigationNode>();
		this.firstNode.flatTree(flat);
		this.nodes = flat.toArray(toArrayMappingTransactionNode);
	}
	
    /*
     * When reusing beans, the transformation needs all key columns defined.
     * A exception is thrown if there is NO key column.
     */	
	public void checkKeys(Table table, List<Function> columns, boolean reuse){
	    if(!reuse) {
	        return;
	    }

        for(Column<?> keyColumn : table.getKeyColumns()){
            boolean noKey = true;
            for (Function column : columns) {
                if(column instanceof ColumnHolder){
                    ColumnHolder ch = (ColumnHolder) column;
                    if (keyColumn.equals(ch.getColumn())) {
                        noKey = false;
                        break;
                    }
                }
            }
            if (noKey)
                throw new PersistenceException("Key columns not found for " + table.toString()
                    + ". When transforming to a object tree and reusing previous beans, ALL key columns must be declared in the select.");
        }
	}

	public void forward() {
		this.depth++;
	}

	public void rewind() {
		this.depth = 0;
	}

}
