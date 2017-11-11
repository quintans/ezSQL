package com.github.quintans.ezSQL.dml;

import static com.github.quintans.ezSQL.toolkit.utils.Misc.length;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.github.quintans.ezSQL.AbstractDb;
import com.github.quintans.ezSQL.common.api.Value;
import com.github.quintans.ezSQL.db.Association;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.Discriminator;
import com.github.quintans.ezSQL.db.Relation;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.sql.RawSql;
import com.github.quintans.ezSQL.sql.SimpleJdbc;

public abstract class DmlBase {

	protected AbstractDb db;
	protected SimpleJdbc simpleJdbc;

	protected Table table;
	protected String tableAlias;
	protected List<Join> joins;
	protected Condition condition;
	protected Map<String, Object> parameters = new LinkedHashMap<String, Object>();

	public static final String JOIN_PREFIX = "j";
	protected AliasBag joinBag = new AliasBag(JOIN_PREFIX);
	protected static final String PREFIX = "t";

	protected String lastFkAlias = null;
	protected Join lastJoin = null;

	protected RawSql rawSql;

	protected List<Condition> discriminatorConditions = null;

	private int rawIndex = 0;

    /**
     * list with the associations of the current path
     */
    protected List<PathElement> path = null;

	public int nextRawIndex() {
		return ++this.rawIndex;
	}

	protected class PathCondition {
		private List<Function> columns;
		private List<Condition> conditions;

		public List<Function> getColumns() {
            return columns;
        }

        public void setColumns(List<Function> columns) {
            this.columns = columns;
        }

        public List<Condition> getConditions() {
			return this.conditions;
		}

		public void setConditions(List<Condition> conditions) {
			this.conditions = conditions;
		}

		public void addCondition(Condition condition) {
			if (this.conditions == null)
				this.conditions = new ArrayList<Condition>();
			this.conditions.add(condition);
		}

		public void addConditions(List<Condition> conditions) {
			if (this.conditions == null)
				this.conditions = new ArrayList<Condition>();
			this.conditions.addAll(conditions);
		}
	}

	public DmlBase(AbstractDb db, Table table) {
		this.db = db;
		this.simpleJdbc = new SimpleJdbc(db.getJdbcSession());

		this.table = table;
		this.tableAlias = PREFIX + "0";

		if(table != null) {
			List<Condition> conditions = table.getConditions();
			if (conditions != null) {
				this.discriminatorConditions = new ArrayList<Condition>(conditions);
			}
		}
	}	

	public AbstractDb getDb() {
		return this.db;
	}
	
	public Table getTable() {
		return this.table;
	}

	public String getTableAlias() {
		return this.tableAlias;
	}

	public void setTableAlias(String alias) {
		this.tableAlias = alias;
		this.rawSql = null;
	}

	public List<Join> getJoins() {
		return this.joins;
	}

	public Map<String, Object> getParameters() {
		return this.parameters;
	}

	public Object getParameter(Column<?> column) {
	   return this.parameters.get(column.getAlias());
	}

	public Condition getCondition() {
		return this.condition;
	}

	/**
	 * Sets the value of parameter to the column.<br>
	 * Converts null values to NullSql type or Values interfaces to its value
	 * 
	 * @param col
	 *            The column
	 * @param value
	 *            The value to set wrapped in a Parameter
	 */
	public void setParameter(Column<?> col, Object value) {
		String name = col.getAlias();

		if(value == null) {
		    value = col.getType();
        }
		
		setParameter(name, value);
	}

    public void setParameter(String name, Object value) {
        if (value instanceof Value<?>) {
            Value<?> e = (Value<?>) value;
            value = e.value();
        }
        this.parameters.put(name, value);
    }
	
	/**
	 * das listas de grupos de Foreign Keys (caminhos),
	 * obtem as Foreign Keys correspondentes ao caminho comum mais longo
	 * que se consegue percorrer com o grupo de Foreign Keys passado
	 * 
	 * @param cachedAssociation
	 *            listas de grupos de Foreign Keys (caminhos)
	 * @param associations
	 *            grupo de Foreign Keys para comparar
	 * @return Foreign Keys correspondentes ao caminho comum mais longo que se
	 *         consegue percorrer
	 */
	public static PathElement[] deepestCommonPath(List<PathElement[]> cachedAssociation, List<PathElement> associations) {
		List<PathElement> common = new ArrayList<PathElement>();

		if (associations != null) {
			for (PathElement[] path : cachedAssociation) {
				// finds the common start portion in this path
				List<PathElement> temp = new ArrayList<PathElement>();
				for (int depth = 0; depth < path.length; depth++) {
					PathElement pe = path[depth];
					if (depth < associations.size()) {
						PathElement pe2 = associations.get(depth);
						if ((pe2.isInner() == null || pe2.isInner().equals(pe.isInner())) && pe2.getBase() != null && pe2.getBase().equals(pe.getBase()))
							temp.add(pe);
						else
							break;
					} else
						break;
				}
				// if common portion is larger than the previous one, use it
				if (temp.size() > common.size()) {
					common = temp;
				}
			}
		}

		return common.toArray(new PathElement[common.size()]);
	}

	public String getAliasForAssociation(Association association) {
		if (this.joinBag != null)
			return this.joinBag.getAlias(association);
		else
			return null;
	}
	
	/**
     * Indicates that the current association chain should be used to join only.
     * A table end alias can also be supplied.
	 * 
	 * @param paths
	 * @param fetch
	 */
	protected void joinTo(List<PathElement> paths, boolean fetch) {
		if (paths != null) {
		    this.addJoin(paths, null, fetch);
		    
			PathCondition[] cache = buildPathConditions(paths);
			// process the acumulated conditions
			List<Condition> firstConditions = null;
			for (int index = 0; index < cache.length; index++) {
				PathCondition pathCondition = cache[index];
				if (pathCondition != null) {
					List<Condition> conds = pathCondition.getConditions();
					// adjustTableAlias()
					if (conds != null) {
						// index == 0 applies to the starting table
						if (index == 0) {
							// already with the alias applied
							firstConditions = conds;
						}
						else {
							//addJoin(null, pathCondition.getPath());
							if (firstConditions != null) {
	                            // add the criterias restriction refering to the table,
	                            // due to association discriminator
								conds = new ArrayList<Condition>(conds);
								conds.addAll(firstConditions);
								firstConditions = null;
							}
							applyOn(paths.subList(0, index), Definition.and(conds));
						}
					}
					
	                if (pathCondition.getColumns() != null) {
	                    this.applyInclude(paths.subList(0, index), pathCondition.getColumns());
	                }
					
				}
			}
		}
	}

	protected PathCondition[] buildPathConditions(List<PathElement> paths) {
		// see if any targeted table has discriminator columns
		int index = 0;
		List<Condition> tableConditions = null;
		PathCondition[] cache = new PathCondition[paths.size() + 1];

		// the path condition on position 0 refers the condition on the FROM table
		// both ends of Discriminator conditions (association origin and destination tables) are treated in this block
		for (PathElement pe : paths) {
			index++;

	        PathCondition pc = null;
	        if (pe.getCondition() != null) {
	            pc = new PathCondition();
	            pc.addCondition(pe.getCondition());
	            cache[index] = pc;
	        }

	        // table discriminator on target
			tableConditions = pe.getBase().getTableTo().getConditions();
			if (tableConditions != null) {
                if (pc == null) {
                    pc = new PathCondition();
                    cache[index] = pc;
                }
				cache[index].addConditions(tableConditions);
			}
			
	        // references column Includes
	        if (pe.getColumns() != null) {
	            if (pc == null) {
	                pc = new PathCondition();
	                cache[index] = pc;
	            }
	            pc.setColumns(pe.getColumns());
	        }
		}

	    // process criterias from the association discriminators
	    String fkAlias = this.getTableAlias();
	    index = 0;
	    for (PathElement pe : paths) {
	        index++;
	        List<Discriminator> discriminators = pe.getBase().getDiscriminators();
	        if (discriminators != null) {
	            PathCondition pc = cache[index];
	            if (pc == null) {
	                pc = new PathCondition();
	                cache[index] = pc;
	            }

	            if (pe.getBase().getDiscriminatorTable().equals(pe.getBase().getTableTo())) {
	                for (Discriminator v : discriminators) {
	                    pc.addCondition(v.getCondition());
	                }
	            } else {
	                // force table alias for the first criteria
                    for (Discriminator v : discriminators) {
	                    Condition crit = v.getCondition();
	                    crit.setTableAlias(fkAlias);
	                    pc.addCondition(crit);
	                }
	            }
	        }
	        fkAlias = this.joinBag.getAlias(pe.getDerived());
	    }

	    return cache;
	}

	// guarda os caminhos(associacao) já percorrida
	protected List<PathElement[]> cachedAssociation = new ArrayList<PathElement[]>();

	protected List<PathElement> addJoin(List<PathElement> associations, PathElement[] common, boolean fetch) {
		List<PathElement> local = new ArrayList<PathElement>();

		if(common == null) {
		    common = deepestCommonPath(this.cachedAssociation, associations);
		}

		if (this.joins == null)
			this.joins = new ArrayList<Join>();

		// guarda os novos
		// List<ForeignKey> newFks = new ArrayList<ForeignKey>();
		// cria copia, pois os table alias vao ser definidos
		Association fks[] = new Association[associations.size()];
		Association lastFk = null;
		boolean matches = true;
		int f = 0;
		for (PathElement pe : associations) {
			Association association = pe.getBase();
			Association lastCachedFk = null;
			if (matches && f < common.length) {
				if (common[f].getBase().equals(association))
					lastCachedFk = common[f].getDerived();
				else
					matches = false;
			} else
				matches = false;

			if (lastCachedFk == null) {
				// copia para atribuir os alias para esta query
				fks[f] = association.bareCopy();

				/**
				 * processa as associações
				 * o alias do lado inicial (from) da primeira associação fica
				 * com o valor firstAlias (valor da tabela principal)
				 * o alias do lado final da última associação, fica com o valor
				 * de lastAlias, se este não for nulo
				 */
	            String fkAlias;
	            if (f == 0) {
	                fkAlias = this.tableAlias;
	            } else {
	                fkAlias = this.joinBag.getAlias(lastFk);
	            }
				if (fks[f].isMany2Many()) {
					Association fromFk = fks[f].getFromM2M();
					Association toFk = fks[f].getToM2M();

					prepareAssociation(
						fkAlias,
						this.joinBag.getAlias(fromFk),
						fromFk);
					
					if (pe.getPreferredAlias() == null) {
						fkAlias = this.joinBag.getAlias(toFk);
					} else {
						fkAlias = pe.getPreferredAlias();
						this.joinBag.setAlias(toFk, pe.getPreferredAlias());
					}
					
					prepareAssociation(
						this.joinBag.getAlias(fromFk),
						fkAlias,
						toFk);
					lastFk = toFk;
				} else {
					String fkAlias2;
					
					if (pe.getPreferredAlias() == null) {
						fkAlias2 = this.joinBag.getAlias(fks[f]);
					} else {
						fkAlias2 = pe.getPreferredAlias();
						this.joinBag.setAlias(fks[f], pe.getPreferredAlias());
					}					
					prepareAssociation(
					        fkAlias,
					        fkAlias2,
						fks[f]);
					lastFk = fks[f];
				}

			} else {
				// a lista principal sempre com a associaccao many-to-many
				fks[f] = lastCachedFk;
				// define o fk anterior
				if (fks[f].isMany2Many()) {
					lastFk = fks[f].getToM2M();
				} else
					lastFk = lastCachedFk;
			}
			pe.setDerived(fks[f]);
			local.add(pe); // cache it

			f++;
		}

		// only caches if the path was different
		if (!matches) {
			this.cachedAssociation.add(local.toArray(new PathElement[0]));
		}
		
		// determina o alias do último join
		this.lastFkAlias = this.joinBag.getAlias(lastFk);

		this.lastJoin = new Join(local, fetch);
		this.joins.add(this.lastJoin);

		return local;
	}

	private void prepareAssociation(String aliasFrom, String aliasTo, Association currentFk) {
        currentFk.setAliasFrom(aliasFrom);
        currentFk.setAliasTo(aliasTo);
        for (Relation rel : currentFk.getRelations()) {
            rel.getFrom().setTableAlias(aliasFrom);
            rel.getTo().setTableAlias(aliasTo);
        }
	}

	protected DmlBase where(Condition restriction) {
		List<Condition> conditions = new ArrayList<Condition>();
		conditions.add(restriction);
		where(conditions);
		return this;
	}

	protected DmlBase where(Condition... restrictions) {
		if (restrictions != null) {
			List<Condition> conditions = new ArrayList<Condition>();
			for (Condition restriction : restrictions)
				conditions.add(restriction);
			where(conditions);
		}
		return this;
	}

	protected DmlBase where(List<Condition> restrictions) {
		if (restrictions != null) {
			List<Condition> conditions = new ArrayList<Condition>();
			if (this.discriminatorConditions != null) {
				conditions.addAll(this.discriminatorConditions);
			}

			conditions.addAll(restrictions);
			if (!conditions.isEmpty()) {
				applyWhere(Definition.and(conditions));
			}
		}
		return this;
	}

	/**
	 * condição a usar na associação imediatamente anterior
	 * 
	 * @param chain
	 * @param condition
	 */
	protected void applyOn(List<PathElement> chain, Condition condition) {
		if (chain != null && chain.size() > 0) {
		    PathElement pe = chain.get(chain.size() - 1);
			Condition copy = (Condition) condition.clone();

			Association fk = pe.getDerived();
	        String fkAlias;
	        if (fk.isMany2Many()) {
	            fkAlias = this.joinBag.getAlias(fk.getToM2M());
	        } else {
	            fkAlias = this.joinBag.getAlias(pe.getDerived());
	        }			
			copy.setTableAlias(fkAlias);

			replaceRaw(copy);
			pe.setCondition(copy);

			this.rawSql = null;
		}
	}
	
	protected void applyInclude(List<PathElement> chain, List<Function> tokens) {
	    int len = length(chain);
        if (len > 0) {
            PathElement pe = chain.get(len-1);
	        Association fk = pe.getDerived();
	        String fkAlias = this.joinBag.getAlias(fk.isMany2Many() ? fk.getToM2M() : pe.getDerived());
	        for (Function token : tokens) {
	            token.setTableAlias(fkAlias);
	        }

	        this.rawSql = null;
	    }
	}	

	// WHERE ===
	protected void applyWhere(Condition restriction) {
		Condition function = (Condition) restriction.clone();
		replaceRaw(function);
		function.setTableAlias(this.tableAlias);

		this.condition = function;

		this.rawSql = null;
	}

	protected String dumpParameters(Map<String, Object> map) {
		StringBuilder sb = new StringBuilder();

		for (Map.Entry<String, Object> entry : map.entrySet()) {
			if(entry.getKey().endsWith("$")) {
				// secret
			    sb.append(String.format("[%s=****]", entry.getKey()));
			} else {
				sb.append(String.format("[%s=%s]", entry.getKey(), entry.getValue()));
			}
		}

		return sb.toString();
	}

	protected Driver driver() {
		return this.db.getDriver();
	}

	public SimpleJdbc getSimpleJdbc() {
		return this.simpleJdbc;
	}

	public abstract RawSql getSql();

	protected Connection connection() {
		return this.db.getConnection();
	}

    protected void debug(Logger logger, String fqcn, String format, Object... args) {
        if (logger.isDebugEnabled()) {
            logger.log(fqcn, Level.DEBUG, String.format(format, args), null);
        }
    }
    
	protected void debugTime(Logger logger, String fqcn, long now) {
		if (logger.isDebugEnabled()) {
			logger.log(fqcn, Level.DEBUG,
				"executed in: " + (System.nanoTime() - now)/1e6 + "ms",
				null);
		}
	}

	protected void debugSQL(Logger logger, String fqcn, String sql) {
		if (logger.isDebugEnabled()) {
		    String caller = callerName(Thread.currentThread().getStackTrace(), fqcn);
			logger.log(fqcn, Level.DEBUG,
				String.format("%s\n\tSQL: %s\n\tparameters: %s", caller, sql, dumpParameters(this.parameters)),
				null);
		}
	}
	
	protected String callerName(StackTraceElement[] stack, String fqcn) {
	    for(int i = stack.length - 1; i >= 0; i--) {
	        if(stack[i].getClassName().equals(fqcn)){
	            StackTraceElement ste = stack[i+1];
	            return ste.getClassName() + "." + ste.getMethodName() + ":" + ste.getLineNumber();
	        }
	    }
	    return "[Unable to determine caller]";
	}
	
    // CONDITIONS
    /**
     * replaces RAW with PARAM
     * 
     * @param token
     */
    protected void replaceRaw(Function token) {
        Function[] members = token.getMembers();
	    if (EFunction.RAW.equals(token.getOperator())) {
	        this.rawIndex++;
	        String parameter = this.tableAlias + "_R" + this.rawIndex;
	        setParameter(parameter, token.getValue());
	        token.setOperator(EFunction.PARAM);
	        token.setValue(parameter);
	    } else if (EFunction.SUBQUERY.equals(token.getOperator())) {
	        Query subquery = (Query) token.getValue();
	        // copy the parameters of the subquery to the main query
	        Map<String, Object> pars = subquery.getParameters();
	        for(Entry<String, Object> entry : pars.entrySet()){
	            setParameter(entry.getKey(), entry.getValue());
	        }
	    } else {
	        if (members != null) {
	            for (int i = 0; i < members.length; i++) {
	                if (members[i] != null) {
	                    this.replaceRaw(members[i]);
	                }
	            }
	        }
	    }
	}
}
