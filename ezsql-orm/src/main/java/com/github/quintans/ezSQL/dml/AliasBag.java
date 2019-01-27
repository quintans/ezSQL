package com.github.quintans.ezSQL.dml;

import java.util.LinkedHashMap;
import java.util.Map;

import com.github.quintans.ezSQL.db.Association;

/**
 *  The goal of this class is to keep track of alias assigned for the different JOINs.
 *  Keep in mind that the same foreign key can have different alias.
 */
public class AliasBag {
	protected String prefix;
	protected int counter = 1;
	protected Map<Association, String> bag = new LinkedHashMap<Association, String>();

	public AliasBag(String prefix) {
		this.prefix = prefix;
	}

	public void setAlias(Association fk, String alias) {
		this.bag.put(fk, alias);
	}

	public String getAlias(Association fk) {
		return bag.computeIfAbsent(fk, k ->  this.prefix + (this.counter++));
	}

}
