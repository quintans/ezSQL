package pt.quintans.ezSQL.transformers;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import pt.quintans.ezSQL.common.api.Updatable;
import pt.quintans.ezSQL.db.Association;
import pt.quintans.ezSQL.dml.Query;
import pt.quintans.ezSQL.exceptions.PersistenceException;
import pt.quintans.ezSQL.toolkit.utils.Holder;

public class DomainBeanTransformer<T> extends BeanTransformer<T> {
	private boolean reuse = false;
	private Navigation navigation = null;
	private Map<String, Map<String, BeanProperty>> cachedBeanMappings = new HashMap<String, Map<String, BeanProperty>>();
	private Map<Object, Object> beans = null;

	public DomainBeanTransformer(Query query, Class<T> klass, boolean reuse) {
		super(query, klass);
		this.reuse = reuse;
		if (reuse)
			this.beans = new HashMap<Object, Object>();
	}

	@Override
	public Collection<T> beforeAll(ResultSet resultSet) {
		this.navigation = new Navigation();
		this.navigation.prepare(getQuery(), this.reuse);

		Collection<T> result = this.reuse ? new LinkedHashSet<T>() : new ArrayList<T>();
		return result;
	}

	@Override
	public void onTransformation(Collection<T> result, T object) {
		this.navigation.rewind();
		if (object != null && (!this.reuse || !result.contains(object)))
			result.add(object);
	}

	@Override
	public void afterAll(Collection<T> result) {
		this.navigation = null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public T transform(ResultSet rs, int[] columnTypes) throws SQLException {
		return (T) transformBean(rs, getClazz(), getQuery().getTableAlias());
	}

	/**
	 * When reuse is false, the full object tree will be created 
	 * to provide a way to reach properties that are far (more than one association.) from the main table.
	 * If after a certain point a branch is empty, that part of the branch is dropped. 
	 * 
	 * @param rs
	 * @param type
	 * @param alias
	 * @return
	 * @throws SQLException
	 */
	@SuppressWarnings("unchecked")
	private Object transformBean(ResultSet rs, Class<?> type, String alias) throws SQLException {
		Map<String, BeanProperty> lastProps = getCachedProperties(rs, alias, type);
		Object bean = null;
		Holder<Boolean> emptyBean = new Holder<Boolean>(Boolean.TRUE);
		if (this.reuse) {
			// for performance, loads only key, because it's sufficient for searching the cache
			bean = loadBeanKeys(rs, type, lastProps, true);
			if (bean != null) {
				// searches the cache
				Object b = this.beans.get(bean);
				// if found, use it
				if (b != null) {
					bean = b;
				} else {
					bean = loadBeanKeys(rs, bean, lastProps, false);
					if (bean != null)
						this.beans.put(bean, bean);
				}
			}
			emptyBean.set(Boolean.FALSE);
		} else {
			bean = toBean(rs, type, lastProps, emptyBean);
		}

		if (bean == null) {
		    ignoreRemaningBranch();
			return null;
		}

		boolean emptyAssoc = true;
		List<Association> fks = forwardBranches();
		if (fks != null) {
			Class<?> subType = null;
			for (Association fk : fks) {
				BeanProperty bp = lastProps.get(alias + "." + fk.getAlias());
				if (bp != null) {
					if (bp.isMany()) { // Collection
						subType = bp.getGenericClass();
					} else
						subType = bp.getKlass();
					Object o = transformBean(rs, subType, fk.isMany2Many() ? fk.getToM2M().getAliasTo() : fk.getAliasTo());
					try {
                        // in case the bean implements Updatable, it will mark the property as dirty
						if (o != null) {
							if (bp.isMany()) { // Collection
								Collection<Object> collection = (Collection<Object>) bp.getReadMethod().invoke(bean);
								if (collection == null) {
								    collection = new LinkedHashSet<Object>();
   	                                bp.invokeWriteMethod(bean, collection);
								}

								if (!this.reuse || !collection.contains(o)) {
									collection.add(o);
								}

							} else {
	                            bp.invokeWriteMethod(bean, o);
							}
							
							emptyAssoc = false;
						}
					} catch (Exception e) {
                        throw new PersistenceException("Unable to write to " + bean.getClass().getSimpleName() + "." + bp.getWriteMethod().getName(), e);
					}
				}
			}
		}

        /*
         * if the bean and all of its associations are null then we can safely ignore this bean.
         * No include() columns were found.
         */
		if(emptyBean.get() && emptyAssoc){
		    return null;
		} else {
	        if(bean instanceof Updatable) {
	            ((Updatable) bean).clear();
	        }
		    return bean;
		}
	}

	@Override
	protected boolean discardIfKeyIsNull() {
		return true;
	}

	protected <E> E loadBeanKeys(ResultSet rs, Class<E> type, Map<String, BeanProperty> properties, boolean onlyKeys) throws SQLException {
		try {
			E obj = type.newInstance();
			return loadBeanKeys(rs, obj, properties, onlyKeys);
		} catch (Exception e) {
            throw new PersistenceException("Unable to create bean " + type.getCanonicalName(), e);
		}
	}

	protected <E> E loadBeanKeys(ResultSet rs, E obj, Map<String, BeanProperty> properties, boolean onlyKeys) throws SQLException {
		boolean keyless = onlyKeys;
		for (Map.Entry<String, BeanProperty> entry : properties.entrySet()) {
			BeanProperty bp = entry.getValue();
			if (bp.getPosition() > 0 && bp.isKey() == onlyKeys) {
				keyless = false;
				int position = bp.getPosition() + getPaginationColumnOffset();
				Class<?> klass = bp.getKlass();
				Object val = driver().fromDb(rs, position, bp.getSqlType(), klass);

					if (val != null) {
		                try {
                            bp.invokeWriteMethod(obj, val);
		                } catch (Exception e) {
		                    throw new PersistenceException("Unable to write to " + obj.getClass().getSimpleName() + "." + bp.getWriteMethod().getName(), e);
		                }
					}
					else if (onlyKeys && bp.isKey()) // if any key is null, the bean is null. ex: a bean coming from a outer join
						return null;
			}
		}

		// if it didn't found any key property, return null
		return keyless ? null : obj;
	}

	private Map<String, BeanProperty> getCachedProperties(ResultSet rs, String alias, Class<?> type) {
		// String key = alias + "." + type.getName();
		String key = alias;
		Map<String, BeanProperty> properties = this.cachedBeanMappings.get(key);
		if (properties == null) {
			properties = populateMapping(rs, alias, type);
			this.cachedBeanMappings.put(key, properties);
		}

		return properties;
	}

	/**
	 * return the list o current branches and moves forward to the next list
	 * 
	 * @return the current list of branches
	 */
	protected List<Association> forwardBranches() {
		List<NavigationNode> assocs = this.navigation.getBranches();
		List<Association> list = null;
		if (assocs != null) {
			list = new ArrayList<Association>();
			for (NavigationNode assoc : assocs)
				list.add(assoc.getForeignKey());
		}
		this.navigation.forward(); // move to next branches
		return list;
	}
	
	protected void ignoreRemaningBranch() {
        List<NavigationNode> assocs = this.navigation.getBranches();
        this.navigation.forward();
        if(assocs != null) {
            ignoreRemaningBranch();
        }
	}
}
