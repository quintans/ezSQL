package pt.quintans.ezSQL.sql;

import java.util.List;
import java.util.Map;

/**
 * Class that wrapps the convertion from a SQL with named parameters to a JDBC standard SQL
 * 
 * @author paulo.quintans
 *
 */
public class RawSql {
    private ParsedSql parsedSql;
    private String sql;
    
	public RawSql(ParsedSql parsedSql) {
        this.parsedSql = parsedSql;
        this.sql = NamedParameterUtils.substituteNamedParameters(parsedSql, null);
    }

	/**
	 * getter for the JDBC SQL
	 * @return
	 */
	public String getSql() {
		return sql;
	}


	/**
	 * getter for the values
	 * @return
	 */
	public List<?> getNames() {
		return parsedSql.getParameterNames();
	}

	public String getOriginalSql() {
        return parsedSql.getOriginalSql();
    }
	
    public Object[] buildValues(Map<String, Object> params){
        return NamedParameterUtils.buildValueArray(parsedSql, params);
	}
	
}
