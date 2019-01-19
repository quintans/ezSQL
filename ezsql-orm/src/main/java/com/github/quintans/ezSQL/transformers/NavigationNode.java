package com.github.quintans.ezSQL.transformers;

import java.util.ArrayList;
import java.util.List;

import com.github.quintans.ezSQL.db.Association;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.toolkit.utils.Holder;

public class NavigationNode {
	private Association foreignKey = null;
	private List<NavigationNode> branches = null;

	public String toString(){
		return foreignKey != null ? foreignKey.getAlias() : ".";
	}
	
	public NavigationNode(){
	}
	
	public Association getForeignKey() {
		return foreignKey;
	}

	public void setForeignKey(Association foreignKey) {
		this.foreignKey = foreignKey;
	}

	public List<NavigationNode> getBranches() {
		return branches;
	}

	/**
	 * To ensure that the several joins are fit correctly and evaluated correctly,
	 * a tree is built, solely for validation.
	 *
	 * @param fks
	 * @param table
	 */
	public void buildTree(Association fks[], Holder<Table> table){
		if(branches == null)
			branches = new ArrayList<NavigationNode>();
				
		NavigationNode found = null;
		for(NavigationNode node : branches){
			if( fks[0].path().equals(node.getForeignKey().path())){
				found = node;
				break;
			}
		}
	
		if(found == null){
			// new branch
			found = new NavigationNode();
			found.setForeignKey(fks[0]);
			branches.add(found);
			table.set(fks[0].getTableTo());
		}
		
		if(fks.length > 1)
			found.buildTree(dropFirst(fks), table);
		
	}
		
	/**
	 * builds a list of all the nodes of the tree por order of entry
	 *
	 * @param flat
	 */
	public void flatTree(List<NavigationNode> flat){
		flat.add(this);
		if(branches != null){
			for(NavigationNode node : branches){
				node.flatTree(flat);
			}
		}
	}
	
	private Association[] dropFirst(Association[] fks){
		if(fks.length == 0)
			return fks;
		else if(fks.length == 1)
			return new Association[0];
		else {
			Association split[] = new Association[fks.length - 1];
			System.arraycopy(fks, 1, split, 0, split.length);
			return split;
		}
	}
	
}
