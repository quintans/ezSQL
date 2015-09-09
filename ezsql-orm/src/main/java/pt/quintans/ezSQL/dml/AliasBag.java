package pt.quintans.ezSQL.dml;

import java.util.LinkedHashMap;
import java.util.Map;

import pt.quintans.ezSQL.db.Association;

/**
 * 
 * @author PQP
 * 
 *         O objectivo desta classe é fornecer a funcionalidade de obtenção dos MESMOS alias ao percorrer os JOINS
 *         Nota: duas listas de joins, mesmo tendo ForeignKey's iguais, produzem alias diferentes
 */
public class AliasBag {
	protected String prefixo = "b";
	protected int contador = 1;
	protected Map<Association, String> bag = new LinkedHashMap<Association, String>();

	public AliasBag(String prefixo) {
		super();
		this.prefixo = prefixo;
	}

	public void setAlias(Association fk, String alias) {
		this.bag.put(fk, alias);
	}

	public String getAlias(Association fk) {
		String alias = this.bag.get(fk);
		if (alias == null) {
			alias = this.prefixo + (this.contador++);
			this.bag.put(fk, alias);
		}
		return alias;
	}

	public boolean has(Association fk) {
		return this.bag.get(fk) != null;
	}
}
