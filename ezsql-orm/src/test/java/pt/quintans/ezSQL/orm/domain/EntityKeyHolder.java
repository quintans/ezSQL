package pt.quintans.ezSQL.orm.domain;

import pt.quintans.ezSQL.toolkit.utils.HashCodeUtil;

public class EntityKeyHolder {
	private Class<?> clazz;
	private Object[] keys;
	int _forHash = 0;

	public EntityKeyHolder(Class<?> clazz) {
		this.clazz = clazz;			
	}

	public Class<?> getClazz() {
		return clazz;
	}

	public Object[] getKeys() {
		return keys;
	}

	public void setKeys(Object[] keys) {
		_forHash = 0; // reset hash
		this.keys = keys;
	}

	@Override
	public boolean equals(Object o){
		if(this == o)
			return true;
		
		if(!(o instanceof EntityKeyHolder) || o == null)
			return false;
		
		EntityKeyHolder ekh = (EntityKeyHolder) o;
		
		if(!clazz.equals(ekh.getClazz()))
			return false;

		Object[] ekhKeys = ekh.getKeys();
		
		if(keys.length != ekhKeys.length)
			return false;
		
		for(int i = 0; i < keys.length; i++){
			if(!keys[i].equals(ekhKeys[i]))
				return false;
		}
			
		return true;
	}
	
	@Override  
    public int hashCode() {
		if(_forHash == 0){
			_forHash = HashCodeUtil.SEED;
			_forHash = HashCodeUtil.hash( _forHash, clazz );
			_forHash = HashCodeUtil.hash( _forHash, keys );
		}

		return _forHash;
	}
	
	@Override  
    public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("{");
		sb.append("class: ").append(clazz.toString());
		for(Object key : keys){
			sb.append(", ").append(key.toString());
		}
		sb.append("}");
		
		return sb.toString();
	}
}
