package pt.quintans.ezSQL.orm.domain;

public abstract class CachedEntity {
	protected EntityKeyHolder ekh;
	
	public CachedEntity(){
	}	
	
	public EntityKeyHolder keyHolder(){
		if(ekh == null)
			ekh = new EntityKeyHolder(this.getClass());
		
		ekh.setKeys(allKeys());
		
		return ekh;
	}
	
	public abstract Object[] allKeys();
	
}
