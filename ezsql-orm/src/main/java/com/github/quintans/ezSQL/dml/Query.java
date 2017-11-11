package com.github.quintans.ezSQL.dml;

import static com.github.quintans.ezSQL.toolkit.utils.Misc.length;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;

import com.github.quintans.ezSQL.AbstractDb;
import com.github.quintans.ezSQL.db.Association;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.exceptions.PersistenceException;
import com.github.quintans.ezSQL.sql.RawSql;
import com.github.quintans.ezSQL.transformers.AbstractRowTransformer;
import com.github.quintans.ezSQL.transformers.BeanTransformer;
import com.github.quintans.ezSQL.transformers.DomainBeanTransformer;
import com.github.quintans.ezSQL.transformers.IProcessor;
import com.github.quintans.ezSQL.transformers.IRowTransformer;
import com.github.quintans.ezSQL.transformers.IRowTransformerFactory;
import com.github.quintans.ezSQL.transformers.SimpleAbstractRowTransformer;

public class Query extends DmlBase {
    private static final Logger LOG = Logger.getLogger(Query.class);
    private static final String FQCN = Query.class.getName();

    public static final String FIRST_RESULT = "first";
    public static final String LAST_RESULT = "last";

    private String alias;
    private Query subquery;
    private boolean distinct;
    private List<Function> columns = new ArrayList<Function>();
    private List<Order> orders;
    private List<Union> unions;
    // saves position of columnHolder
    private int[] groupBy = null;

    private int skip;
    private int limit;

    private Function lastFunction = null;
    private Order lastOrder = null;

    private Condition having;

    private boolean useTree;

    public Query(AbstractDb db, Table table) {
        super(db, table);
    }

    public Query(Query subquery) {
        super(subquery.getDb(), null);
        this.subquery = subquery;
        // copy the parameters of the subquery to the main query
        for (Entry<String, Object> entry : subquery.getParameters().entrySet()) {
            this.setParameter(entry.getKey(), entry.getValue());
        }

    }

    public String getAlias() {
        return alias;
    }

    public void alias(String alias) {
        this.alias = alias;
    }

    public void copy(Query other) {
        this.table = other.getTable();
        this.tableAlias = other.getTableAlias();

        if (other.getJoins() != null)
            this.joins = new ArrayList<Join>(other.getJoins());
        if (other.getCondition() != null)
            this.condition = (Condition) other.getCondition().clone();
        if (this.parameters != null)
            this.parameters = new LinkedHashMap<String, Object>(other.getParameters());

        if (other.getSubquery() != null) {
            Query q = other.getSubquery();
            this.subquery = new Query(this.db, q.getTable());
            this.subquery.copy(q);
        }

        this.distinct = other.isDistinct();
        if (other.getColumns() != null)
            this.columns = new ArrayList<Function>(other.getColumns());
        if (other.getOrders() != null)
            this.orders = new ArrayList<Order>(other.getOrders());
        if (other.getUnions() != null)
            this.unions = new ArrayList<Union>(other.getUnions());
        // saves position of columnHolder
        if (other.getGroupBy() != null)
            this.groupBy = other.getGroupBy().clone();

        this.skip = other.getSkip();
        this.limit = other.getLimit();

        this.rawSql = other.rawSql;
    }

    public int getSkip() {
        return this.skip;
    }

    public Query skip(int firstResult) {
        if (firstResult < 0)
            this.skip = 0;
        else
            this.skip = firstResult;
        return this;
    }

    public int getLimit() {
        return this.limit;
    }

    public Query limit(int maxResults) {
        if (maxResults < 0) {
            this.limit = 0;
        } else {
            this.limit = maxResults;
        }
        return this;
    }

    public Query getSubquery() {
        return this.subquery;
    }

    public Query distinct() {
        this.distinct = true;
        this.rawSql = null;
        return this;
    }

    public boolean isDistinct() {
        return this.distinct;
    }

    // COLUMN ===

    public Query all() {
        if (this.table != null) {
            for (Column<?> column : this.table.getColumns()) {
                column(column);
            }
        }
        return this;
    }

    public Query column(Object... cols) {
        if(cols != null && cols.length > 0){
            for(Object col: cols) {
                this.lastFunction = Function.converteOne(col);
                replaceRaw(lastFunction);
        
                this.lastFunction.setTableAlias(this.tableAlias);
                this.columns.add(this.lastFunction);
            }
        }

        this.rawSql = null;

        return this;
    }

    public Query count() {
        column(Definition.count());
        return this;
    }

    public Query count(Object... expr) {
        if(expr != null && expr.length > 0){
            for(Object e : expr) {
                column(Definition.count(e));
            }
        }
        return this;
    }

    /**
     * Defines the alias of the last column, or if none was defined, defines the table alias;
     * 
     * @param alias
     *            The Alias
     * @return The query
     */
    public Query as(String alias) {
        if (this.lastFunction != null) {
            this.lastFunction.as(alias);
        } else if(this.path != null) {
        	this.path.get(this.path.size() - 1).setPreferredAlias(alias);
        } else {
            this.joinBag = new AliasBag(alias + "_" + JOIN_PREFIX);
            this.tableAlias = alias;
        }

        this.rawSql = null;

        return this;
    }

    // ===

    public List<Function> getColumns() {
        return this.columns;
    }

    // WHERE ===
    @Override
    public Query where(Condition restriction) {
        return (Query) super.where(restriction);
    }

    @Override
    public Query where(Condition... restrictions) {
        return (Query) super.where(restrictions);
    }

    @Override
    public Query where(List<Condition> restrictions) {
        return (Query) super.where(restrictions);
    }

    // ===

    // ORDER ===
    private Query order(ColumnHolder columnHolder) {
        if (this.orders == null)
            this.orders = new ArrayList<Order>();

        this.lastOrder = new Order(columnHolder, true);
        this.orders.add(this.lastOrder);

        this.rawSql = null;

        return this;
    }

    /**
     * Order by a column belonging to the driving table<br>
     * If you want to order by a column from the table targeted by the last association, use orderBy
     * 
     * @param column
     * @return
     */
    public Query order(Column<?> column) {
        return order(column, this.tableAlias);
    }
    
    public Query asc(Column<?> column) {
        return order(column, this.tableAlias).asc();
    }

    public Query desc(Column<?> column) {
        return order(column, this.tableAlias).desc();
    }

    public Query order(Column<?> column, String alias) {
        ColumnHolder ch = new ColumnHolder(column);
        if (alias != null) {
            ch.setTableAlias(alias);
        } else {
            ch.setTableAlias(this.tableAlias);
        }

        return order(ch);
    }

    public Query asc(Column<?> column, String alias) {
        return order(column, this.tableAlias).asc();
    }

    public Query desc(Column<?> column, String alias) {
        return order(column, alias).desc();
    }

    /**
     * Order by column belonging to another table. 
     * This might come in handy when the order cannot be declared in the same order as the joins. 
     * 
     * @param column
     *            a coluna de ordenação
     * @param associations
     *            associações para chegar à tabela que contem a coluna de
     *            ordenação
     * @return devolve a query
     */
    public Query order(Column<?> column, Association... associations) {
        List<PathElement> pathElements = new ArrayList<PathElement>();
        for (Association association : associations)
            pathElements.add(new PathElement(association, null));

        return order(column, pathElements);
    }

    private Query order(Column<?> column, List<PathElement> pathElements) {
        PathElement[] common = deepestCommonPath(this.cachedAssociation, pathElements);
        if (common.length == pathElements.size()) {
            return order(column, pathElementAlias(common[common.length - 1]));
        } else
            throw new PersistenceException("The path specified in the order is not valid");

    }

    /**
     * Define a coluna a ordenar. A coluna pertence à última associação
     * definida, <br>
     * ou se não houver nenhuma associação, a coluna pertencente à tabela
     * 
     * @param column
     * @return
     */
    public Query orderBy(Column<?> column) {
    	if (this.path != null) {
    		PathElement last = this.path.get(this.path.size()-1);
    		if (last.getOrders() == null) {
    			last.setOrders(new ArrayList<Order>());
    		}
    		// delay adding order
    		this.lastOrder = new Order(new ColumnHolder(column));
    		last.getOrders().add(this.lastOrder);
    		return this;
    	} else if (this.lastJoin != null)
            return order(column, this.lastJoin.getPathElements());
        else
            return order(column, this.lastFkAlias);
    }

    public Query ascBy(Column<?> column) {
        return orderBy(column).asc();
    }

    public Query descBy(Column<?> column) {
        return orderBy(column).desc();
    }

    public Query order(String column) {
        if (this.orders == null)
            this.orders = new ArrayList<Order>();

        this.lastOrder = new Order(column, true);
        this.orders.add(this.lastOrder);

        this.rawSql = null;

        return this;
    }
    
    public Query asc(String column) {
        return order(column).asc();
    }

    public Query desc(String column) {
        return order(column).desc();
    }    

    /**
     * Define a direção ASCENDENTE da ordem a aplicar na ultima ordem definida
     * 
     * @return this
     */
    public Query asc() {
        asc(true);
        return this;
    }

    /**
     * Define a direção da ordem a aplicar na ultima ordem definida
     * 
     * @param dir
     * @return this
     */
    public Query asc(boolean dir) {
        if (this.lastOrder != null) {
            this.lastOrder.setAsc(dir);

            this.rawSql = null;
        }
        return this;
    }

    /**
     * Define a direção DESCENDENTE da ordem a aplicar na ultima ordem definida
     * 
     * @return
     */
    public Query desc() {
        if (this.lastOrder != null) {
            this.lastOrder.setAsc(false);

            this.rawSql = null;
        }
        return this;
    }

    public List<Order> getOrders() {
        return this.orders;
    }

    // ===

    // JOINS ===


    /**
     * includes the associations as inner joins to the current path
     * 
     * @param inner
     * @param associations
     * @return
     */
    private Query _associate(boolean inner, Association... associations) {
        if(length(associations) == 0){
            throw new PersistenceException("Inner cannot be used with an empty association list!");
        }

        if (this.path == null)
            this.path = new ArrayList<PathElement>();

        Table lastTable = null;
        if(path.size() > 0) {
            lastTable = path.get(path.size() - 1).getBase().getTableTo();
        } else {
            lastTable = table;
        }
        // all associations must be linked by table
        for(Association assoc : associations){
            if(!lastTable.equals(assoc.getTableFrom())){
                StringBuilder sb = new StringBuilder();
                sb.append("Association list ");
                for(Association a : associations){
                    sb.append("[")
                    .append(a.genericPath())
                    .append("]");
                }
                sb.append(" is invalid. Association [")
                .append(assoc.genericPath())
                .append("] must start on table ")
                .append(lastTable.getName());
                throw new PersistenceException(sb.toString());
            }
            lastTable = assoc.getTableTo();
        }
        
        for (Association association : associations) {
            PathElement pe = new PathElement(association, inner);
            this.path.add(pe);
        }

        this.rawSql = null;

        return this;
    }
    
    /**
     * includes the associations as inner joins to the current path.
     * 
     * @param associations
     * @return
     */
    public Query inner(Association... associations) {
        return _associate(true, associations);
    }

    /**
     * includes the associations as outer joins to the current path
     * 
     * @param associations
     * @return
     */
    public Query outer(Association... associations) {
        return _associate(false, associations);
    }

    private void _fetch(List<PathElement> paths) {
        useTree = true;

        if(paths != null) {
            for(PathElement pe : paths) {
            	// includes all columns if there wasn't a previous include
                includeInPath(pe);
            }
        }

        _join(true);
    }    

    /**
     * This will trigger a result that can be dumped in a tree object 
     * using current association path to build the tree result.<br>
     * If no columns where included in this path, it will includes all the columns 
     * of all the tables referred by the association path.
     * 
     * @return
     */
    public Query fetch() {
        _fetch(this.path);

        return this;
    }

    /**
     * This will NOT trigger a result that can be dumped in a tree object.<br>
     * Any included column, will be considered as belonging to the root object.  
     * 
     * @return
     */
    public Query join() {
        _join(false);
        return this;
    }

    private void _join(boolean fetch) {
        if(this.path != null) {
            List<Function> tokens = new ArrayList<Function>();
            for(PathElement pe : this.path) {
                List<Function> funs = pe.getColumns();
                if(funs != null) {
                    for(Function fun : funs){
                        tokens.add(fun);
                        if(!fetch) {
                            fun.setPseudoTableAlias(this.tableAlias);
                        }
                    }
                }
            }
            
            this.columns.addAll(tokens);            
        }
        
        // only after this the joins will have the proper join table alias
        super.joinTo(this.path, fetch);
        
    	// process pending orders
    	if (this.path != null) {
    		for (PathElement pe : this.path) {
    			if (pe.getOrders() != null) {
    				for (Order o : pe.getOrders()) {
    					o.getHolder().setTableAlias(pathElementAlias(pe));
    					this.getOrders().add(o);
    				}
    			}
    		}
    	}        
        this.path = null;
        this.rawSql = null;
    }
    
    private static String pathElementAlias(PathElement pe) {
    	Association derived = pe.getDerived();
    	if (derived.isMany2Many()) {
    		return derived.getToM2M().getAliasTo();
    	} else {
    		return derived.getAliasTo();
    	}
    }    

    /**
     * The same as inner(...).join()
     * 
     * @param associations
     * @return
     */
    public Query innerJoin(Association... associations) {
        return inner(associations).join();
    }

    /**
     * The same as outer(...).join()
     * 
     * @param associations
     * @return
     */
    public Query outerJoin(Association... associations) {
        return outer(associations).join();
    }

    /**
     * Includes any kind of column (table column or function) 
     * referring to the table targeted by the last association. 
     * 
     * @param columns or functions
     * @return
     */
    public Query include(Object... columns) {
        int lenPath = length(this.path);
        if (lenPath > 0) {
            PathElement lastPath = this.path.get(lenPath - 1);
            Table lastTable = lastPath.getBase().getTableTo();
            for(Object c : columns){
                // if it is a columns check if it belongs to the last table
                if(c instanceof Column) {
                    Column<?> col = (Column<?>) c;
                    if(!col.getTable().equals(lastTable)) {
                        throw new PersistenceException(
                                String.format("Column %s does not belong to the table target by the association %s.", 
                                        col.toString(), 
                                        lastPath.getBase().genericPath()));
                    }
                }
            }
            includeInPath(lastPath, columns);
            
            this.rawSql = null;
        } else {
            throw new PersistenceException("There is no current join");
        }
        return this;
    }
    
    private void includeInPath(PathElement lastPath, Object... columns){
    	if(length(columns) > 0 || length(lastPath.getColumns()) == 0) {
	        if (length(columns) == 0) {
	            // use all columns of the targeted table
	            columns = lastPath.getBase().getTableTo().getColumns().toArray();
	        }
	        List<Function> toks = lastPath.getColumns();
	        if(toks == null) {
	            toks = new ArrayList<Function>();
	            lastPath.setColumns(toks);
	        }
	        for(Object c : columns){
	            this.lastFunction = Function.converteOne(c);
	            toks.add(this.lastFunction);
	        }
    	}
    }

    /**
     * includes all column from the table from the last association but the ones declared in this method.
     * 
     * @param columns
     * @return
     */
    public Query exclude(Column<?>... columns) {
        int lenPath = length(this.path);
        if (lenPath > 0) {
            if (length(columns) == 0) {
                throw new PersistenceException("null or empty values was passed");
            }
            Set<Column<?>> cols = this.path.get(lenPath - 1).getBase().getTableTo().getColumns();
            LinkedHashSet<Column<?>> remain = new LinkedHashSet<Column<?>>(cols);
            for(Column<?> c : columns) {
                remain.remove(c);
            }
            
            include(remain.toArray());
        } else {
            throw new PersistenceException("There is no current join");
        }
        return this;
    }    
    /* INCLUDES */

    /**
     * Executa um OUTER join com as tabelas definidas pelas foreign keys.<br>
     * TODAS as colunas das tabelas intermédias são incluidas no select bem como
     * a TODAS as colunas da tabela no fim das associações.<br>
     * 
     * @param associations
     *            as foreign keys que definem uma navegação
     * @return
     */
    public Query outerFetch(Association... associations) {
        outer(associations).fetch();
        return this;
    }

    /**
     * Executa um INNER join com as tabelas definidas pelas foreign keys.<br>
     * TODAS as colunas das tabelas intermédias são incluidas no select bem como
     * TODAS as colunas da tabela no fim das associações.<br>
     * 
     * @param associations
     *            as foreign keys que definem uma navegação
     * @return
     */
    public Query innerFetch(Association... associations) {
        inner(associations).fetch();
        return this;
    }

    /**
     * Restriction to apply to the previous association
     * 
     * @param condition
     *            Restriction
     * @return
     */
    public Query on(Condition... condition) {
        int lenPath = length(this.path);
        if (lenPath > 0) {
            Condition retriction;
            if (length(condition) == 0) {
                throw new PersistenceException("null or empty criterias was passed");
            } else {
                retriction = Definition.and(condition);
            }
            this.path.get(lenPath - 1).setCondition(retriction);

            this.rawSql = null;
        } else {
            throw new PersistenceException("There is no current join");
        }
        return this;
    }

    // =====

    // UNIONS ===
    public Query union(Query query) {
        if (this.unions == null)
            this.unions = new ArrayList<Union>();
        this.unions.add(new Union(query, false));

        this.rawSql = null;

        return this;
    }

    public Query unionAll(Query query) {
        if (this.unions == null)
            this.unions = new ArrayList<Union>();
        this.unions.add(new Union(query, true));

        this.rawSql = null;

        return this;
    }

    public List<Union> getUnions() {
        return this.unions;
    }

    // ===

    // GROUP BY ===
    public Query groupByUntil(int untilPos) {
        int[] pos = new int[untilPos];
        for (int i = 0; i < pos.length; i++)
            pos[i] = i + 1;

        this.groupBy = pos;

        this.rawSql = null;

        return this;
    }

    public Query groupBy(int... pos) {
        this.groupBy = pos;

        this.rawSql = null;

        return this;
    }

    public int[] getGroupBy() {
        return this.groupBy;
    }

    public List<Group> getGroupByFunction() {
        List<Group> groups = null;
        int length = this.groupBy == null ? 0 : groupBy.length;
        if (length > 0) {
            groups = new ArrayList<Group>(length);
            for (int k = 0; k < length; k++) {
                int idx = groupBy[k] - 1;
                groups.add(new Group(idx, columns.get(idx)));
            }
        }
        return groups;
    }

    public Query groupBy(Column<?>... cols) {
        this.rawSql = null;

        if (cols == null || cols.length == 0) {
            this.groupBy = null;
            return this;
        }

        this.groupBy = new int[cols.length];

        int pos = 1;
        for (int i = 0; i < cols.length; i++) {
            for (Function function : this.columns) {
                if (function instanceof ColumnHolder) {
                    ColumnHolder ch = (ColumnHolder) function;
                    if (ch.getColumn().equals(cols[i])) {
                        this.groupBy[i] = pos;
                        break;
                    }
                }
            }
            pos++;

            if (this.groupBy[i] == 0)
                throw new PersistenceException(String.format("Column alias '%' was not found", cols[i]));
        }

        return this;
    }

    public Query groupBy(String... aliases) {
        this.rawSql = null;

        if (aliases == null || aliases.length == 0) {
            this.groupBy = null;
            return this;
        }

        this.groupBy = new int[aliases.length];

        int pos = 1;
        for (int i = 0; i < aliases.length; i++) {
            for (Function function : this.columns) {
                if (aliases[i].equals(function.getAlias())) {
                    this.groupBy[i] = pos;
                    break;
                }
            }
            pos++;

            if (this.groupBy[i] == 0)
                throw new PersistenceException(String.format("Column alias '%' was not found", aliases[i]));
        }

        return this;
    }

    public Condition getHaving() {
        return having;
    }

    /**
     * Adds a Having clause to the query. The tokens are not processed. You will
     * have to explicitly set all table alias.
     * 
     * @param having
     * @return this
     */
    public Query having(Condition... having) {
        if (having != null) {
            this.having = Definition.and(having);
            this.replaceAlias(this.having);
        }

        return this;
    }

    /**
     * replaces ALIAS with the respective select parcel
     * 
     * @param token
     */
    private void replaceAlias(Function token) {
        Function[] members = token.getMembers();
        if (token.getOperator() == EFunction.ALIAS) {
            String alias = (String) token.getValue();
            for (Function v : columns) {
                // full copies the matching
                if (alias.equals(v.getAlias())) {
                    token.as(alias);
                    token.setMembers(v.getMembers());
                    token.setOperator(v.getOperator());
                    token.setTableAlias(v.getTableAlias());
                    token.setValue(v.getValue());
                    break;
                }
            }
            return;
        } else {
            if (members != null) {
                for (Function t : members) {
                    if (t != null) {
                        this.replaceAlias(t);
                    }
                }
            }
        }
    }

    // ======== RETRIVE ==============
    
    /**
     * Every row will be processed by the only method of the supplied object.<br>
     * The Object type for each column is obtained from the types of the method parameters.<br>
     * The processed rows are not collected, so any returning object is discarded.
     *  
     * @param processor A processor object that must have one declared method and this method must at least one parameter.
     */
	public void run(final IProcessor processor) {
        list(createReflectionTransformer(processor));
	}

	public void runOne(final IProcessor processor) {
		fetchUnique(createReflectionTransformer(processor));
	}

	public void runFirst(final IProcessor processor) {
		select(createReflectionTransformer(processor));
	}

    private AbstractRowTransformer<Void> createReflectionTransformer(final Object processor){
		Method[] methods = processor.getClass().getDeclaredMethods();
		if(methods.length != 1) {
			throw new PersistenceException("The supplied object must have one declared method. Found " + methods.length + " methods!" );
		}

		final Method method = methods[0];
	    final Class<?>[] clazzes = method.getParameterTypes();
		if(clazzes.length == 0) {
			throw new PersistenceException("The method " + method.getName() + " must have at least one parameter!");
		}
		
		method.setAccessible(true);
		
        final int offset = driver().paginationColumnOffset(this);

        return new AbstractRowTransformer<Void>(getDb(), true) {
            @Override
            public Void transform(ResultSet rs, int[] columnTypes) throws SQLException {
            	Object[] objs = Query.this.transform(rs, columnTypes, offset, clazzes);
                // reflection call
                try {
					method.invoke(processor, objs);
				} catch (Exception e) {
					Class<?>[] c = new Class<?>[objs.length];
					for(int i = 0; i< objs.length; i++) {
						Object o = objs[i];
						c[i] = o != null ? o.getClass() : null;
					}
					throw new PersistenceException(
							"There was an error while calling the method \""
									+ method.getName() + "\" with "
									+ Arrays.toString(objs) + " -> "
									+ Arrays.toString(c) + ": "
									+ e.getMessage(), 
									e);
				}
                
                return null;
            }
        };
    }
	
    public List<Object[]> listRaw(final Class<?>... clazzes) {
        return list(createRawTransformer(clazzes));
    }

    public Object[] uniqueRaw(final Class<?>... clazzes) {
        return fetchUnique(createRawTransformer(clazzes));
    }
    
    private SimpleAbstractRowTransformer<Object[]> createRawTransformer(final Class<?>... clazzes){
        if(length(clazzes) == 0){
            throw new PersistenceException("Classes must be defined!");
        }
        
        final int offset = driver().paginationColumnOffset(this);
        
        return new SimpleAbstractRowTransformer<Object[]>(getDb(), true) {
            @Override
            public Object[] transform(ResultSet rs, int[] columnTypes) throws SQLException {
            	return Query.this.transform(rs, columnTypes, offset, clazzes);
            }
        };
    }
    
    private Object[] transform(ResultSet rs, int[] columnTypes, int offset, Class<?>... clazzes) throws SQLException {
        int cnt = 0;
        if(clazzes.length > 0)
            cnt = Math.min(columnTypes.length, clazzes.length);
        else
            cnt = columnTypes.length;
        Object objs[] = new Object[cnt];
        for(int i = 0; i < cnt; i++){
            objs[i] = driver().fromDb(rs, i + 1 + offset, columnTypes[i], cnt > 0 ? clazzes[i] : null);
        }
        return objs;
    }
    

    /**
     * Retrives a collection of objects of simple type (not beans). Ex: Boolean,
     * String, enum, ...
     * 
     * @param clazz
     *            class of the object to return
     * @return
     */
    public <T> List<T> listRaw(final Class<T> clazz) {
        final int offset = driver().paginationColumnOffset(this);

        return list(new SimpleAbstractRowTransformer<T>(getDb(), true) {
            @Override
            public T transform(ResultSet rs, int[] columnTypes) throws SQLException {
                return driver().fromDb(rs, 1 + offset, columnTypes[0], clazz);
            }
        });
    }

    public <T> T uniqueRaw(final Class<T> clazz) {
        final int offset = driver().paginationColumnOffset(this);

        return fetchUnique(new SimpleAbstractRowTransformer<T>(getDb(), true) {
            @Override
            public T transform(ResultSet rs, int[] columnTypes) throws SQLException {
                return driver().fromDb(rs, 1 + offset, columnTypes[0], clazz);
            }
        });
    }

    public Boolean uniqueBoolean() {
        final int offset = driver().paginationColumnOffset(this);

        return fetchUnique(new SimpleAbstractRowTransformer<Boolean>(getDb()) {
            @Override
            public Boolean transform(ResultSet rs, int[] columnTypes) throws SQLException {
                return driver().toBoolean(rs, 1 + offset);
            }
        });
    }
    
    public Integer uniqueInteger() {
        final int offset = driver().paginationColumnOffset(this);

        return fetchUnique(new SimpleAbstractRowTransformer<Integer>() {
            @Override
            public Integer transform(ResultSet rs, int[] columnTypes) throws SQLException {
                return rs.getInt(1 + offset);
            }
        });
    }

    public Long uniqueLong() {
        final int offset = driver().paginationColumnOffset(this);

        return fetchUnique(new SimpleAbstractRowTransformer<Long>() {
            @Override
            public Long transform(ResultSet rs, int[] columnTypes) throws SQLException {
                return rs.getLong(1 + offset);
            }
        });
    }

    public Float uniqueFloat() {
        final int offset = driver().paginationColumnOffset(this);

        return fetchUnique(new SimpleAbstractRowTransformer<Float>() {
            @Override
            public Float transform(ResultSet rs, int[] columnTypes) throws SQLException {
                return rs.getFloat(1 + offset);
            }
        });
    }

    public Double uniqueDouble() {
        final int offset = driver().paginationColumnOffset(this);

        return fetchUnique(new SimpleAbstractRowTransformer<Double>() {
            @Override
            public Double transform(ResultSet rs, int[] columnTypes) throws SQLException {
                return rs.getDouble(1 + offset);
            }
        });
    }

    public String uniqueString() {
        final int offset = driver().paginationColumnOffset(this);
        return fetchUnique(new SimpleAbstractRowTransformer<String>() {
            @Override
            public String transform(ResultSet rs, int[] columnTypes) throws SQLException {
                return rs.getString(1 + offset);
            }
        });
    }

    public BigDecimal uniqueBigDecimal() {
        final int offset = driver().paginationColumnOffset(this);
        return fetchUnique(new SimpleAbstractRowTransformer<BigDecimal>() {
            @Override
            public BigDecimal transform(ResultSet rs, int[] columnTypes) throws SQLException {
                return rs.getBigDecimal(1 + offset);
            }
        });
    }

    private <T> T fetchUnique(IRowTransformer<T> rt) {
        RawSql rsql = getSql();
        debugSQL(LOG, FQCN, rsql.getOriginalSql());
        long now = System.nanoTime();
        Map<String, Object> params = db.transformParameters(this.parameters);
        T o = getSimpleJdbc().queryUnique(rsql.getSql(), rt, rsql.buildValues(params));
        debugTime(LOG, FQCN, now);
        return o;
    }

    /**
     * Executes a query and transform the results to the bean type,<br>
     * matching the alias with bean property name.
     * 
     * @param <T>
     *            the bean type
     * @param klass
     *            The bean type
     * @param reuse
     *            Indicates if for the same entity, a new bean should be
     *            created, or reused a previous instanciated one.
     * @return A collection of beans
     */
    public <T> List<T> list(Class<T> klass, boolean reuse) {
        if(useTree){
            return list(new DomainBeanTransformer<T>(this, klass, reuse));
        } else {
            return list(new BeanTransformer<T>(this, klass));
        }
    }

    /**
     * Executes a query and transform the results to the bean type passed as
     * parameter,<br>
     * matching the alias with bean property name. If no alias is supplied, it
     * is used the column alias.
     * 
     * @param klass
     * @return
     */
    public <T> List<T> list(Class<T> klass) {
        return list(klass, true);
    }

    public <T> List<T> list(IRowTransformerFactory<T> factory, boolean reuse) {
        return list(factory.createTransformer(this, reuse));
    }

    public <T> List<T> list(IRowTransformerFactory<T> factory) {
        return list(factory, true);
    }

    /**
     * Executes a query and transform the results according to the transformer
     * 
     * @param <T>
     *            the bean type
     * @param rowMapper
     *            The row transformer
     * @return A collection of transformed results
     */
    public <T> List<T> list(final IRowTransformer<T> rowMapper) {
        // closes any open path
        if(this.path != null) {
            join();
        }

        RawSql rsql = getSql();
        debugSQL(LOG, FQCN, rsql.getOriginalSql());

        Map<String, Object> pars = db.transformParameters(this.parameters);

        long now = System.nanoTime();
        List<T> list = null;
        if (driver().useSQLPagination()) {
            // defining skip and limit as zero, will default to use SQL paginagion (intead of JDBC pagination).
            list = getSimpleJdbc().queryRange(rsql.getSql(), rowMapper, 0, 0, rsql.buildValues(pars));
        } else {
            list = getSimpleJdbc().queryRange(rsql.getSql(), rowMapper, this.skip, this.limit, rsql.buildValues(pars));
        }
        debugTime(LOG, FQCN, now);
        return list;
    }
    
    /**
     * Executes a query and transform the results according to the transformer.<br>
     * If more than one result is returned an Exception will occur. 
     * 
     * @param <T>
     *            the bean type
     * @param rowMapper
     *            The row transformer
     * @return A collection of transformed results
     */
    public <T> T unique(final IRowTransformer<T> rowMapper) {
        // closes any open path
        if(this.path != null) {
            join();
        }
        
        RawSql rsql = getSql();
        debugSQL(LOG, FQCN, rsql.getOriginalSql());

        Map<String, Object> pars = db.transformParameters(this.parameters);

        long now = System.nanoTime();
        T result = getSimpleJdbc().queryUnique(rsql.getSql(), rowMapper, rsql.buildValues(pars));
        debugTime(LOG, FQCN, now);
        return result;
    }
    
    // ======== SELECT (ONE RESULT) ================
    
    public <T> T unique(Class<T> klass) {
        if(useTree) {
            return select(klass);
        } else {
            return unique(new BeanTransformer<T>(this, klass));
        }
    }
    
    public <T> T select(Class<T> klass) {
        return select(klass, true);
    }
    
    public <T> T select(Class<T> klass, boolean reuse) {
        if (useTree) {
            if (reuse) {
                List<T> list = list(new DomainBeanTransformer<T>(this, klass, true));

                if (list.size() == 0)
                    return null;
                else
                    return list.get(0); // first one
            } else {
                return select(new DomainBeanTransformer<T>(this, klass, false));
            }
        } else {
            return select(new BeanTransformer<T>(this, klass));
        }
    }

    public <T> T select(IRowTransformer<T> rowMapper) {
        int holdMax = this.limit;
        limit(1);

        List<T> list = list(rowMapper);

        limit(holdMax);

        if (list.size() == 0)
            return null;
        else
            return list.get(0); // first one
    }
    
    /**
     * SQL String. It is cached for multiple access
     */
    @Override
    public RawSql getSql() {
        if (this.rawSql == null) {
            // if the discriminator conditions have not yet been processed, apply them now
            if (this.discriminatorConditions != null && this.condition == null) {
                where(new ArrayList<Condition>());
            }

            String sql = driver().getSql(this);
            this.rawSql = getSimpleJdbc().toRawSql(sql);
        }

        return this.rawSql;
    }
    
    public Function subQuery(){
        return Definition.subQuery(this);
    }
}
