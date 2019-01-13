package com.github.quintans.ezSQL.driver;

import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.dml.AutoKeyStrategy;
import com.github.quintans.ezSQL.dml.Function;
import com.github.quintans.ezSQL.dml.Query;
import com.github.quintans.jdbc.exceptions.PersistenceException;


/*
NOTA: nenhuma destas implementações suporta insensitive like - UPPER(<coluna>) LIKE UPPER(<string>)
----------------------------------------------------------------
resultset Pagination
in http://issues.apache.org/jira/browse/OPENJPA-759  
 
Some characteristics:
- The idea is to limit the result set returned by SQL query instead of skipping rows
  when traversing the result set in OpenJPA. A similar approach exists
  in OracleDictionary.
- If setMaxResulsts and setFirstResult were called on Query, the SQL query
  [QUERY] is modified as follows:

SELECT * FROM (
  SELECT rr.*, ROW_NUMBER() OVER(ORDER BY ORDER OF rr) AS rn FROM (
    [QUERY]
    FETCH FIRST [m] ROWS ONLY
  ) AS rr
) AS r WHERE rn > [n] ORDER BY rn

- The modified SQL query adds one column to the end of column list in the
  result set. Luckily, I couldn't find any side effects of doing this.
- If only setMaxResults was called on Query, only FETCH FIRST [m] ROWS ONLY
  is appended to SQL query - this is how it works currently.
- The new way of paging will be used only if the database is a UDB 8.1 or later
  because of ORDER OF construct and FETCH FIRST [m] ROWS ONLY in a subselect.
  Maybe some other DB2 flavours could also handle it but I have no access.
- User can fall back to the old behaviour by setting supportsSelectStartIndex
  Dictionary property to false.
   
=======================
in http://www.ibm.com/developerworks/data/library/techarticle/0307balani/0307balani.html   

SELECT * FROM (
  SELECT PRODUCT_ID, PRODUCT_NAME,
    PRODUCT_DESCRIPTION, PRODUCT_PRICE, 	
    rownumber() OVER
    (ORDER BY PRODUCT_ID) AS ROW_NEXT 
    FROM PRODUCT,PRODUCT_CATEGORY
	WHERE
      PRODUCT. PROD_CATEGORY_ID
      = PRODUCT_CATEGORY.CATEGORY_ID 
    AND
      PRODUCT_CATEGORY.CATEGORY_ID = 'Books'
    AND 
      PRODUCT. PRODUCT_DESCRIPTION LIKE 
	   'Application Servers'
  )
AS PRODUCT_TEMP WHERE 
ROW_NEXT BETWEEN ? and ?    
 */

public class DB2Driver extends GenericDriver {
	
	public String getAutoNumberQuery(Column<? extends Number> column) {
		return getAutoNumberQuery(column, false);
	}
	
	public String getCurrentAutoNumberQuery(Column<? extends Number> column) {
		return getAutoNumberQuery(column, true);
    }

	public String getAutoNumberQuery(Column<? extends Number> column, boolean current) {
		if(column.isKey())
			return "select IDENTITY_VAL_LOCAL() from sysibm.sysdummy1";
		else
			throw new PersistenceException(String.format("A função getAutoNumberQuery não reconhece a coluna %s.", column));
    }
	
    @Override
    public AutoKeyStrategy getAutoKeyStrategy() {
        return AutoKeyStrategy.AFTER;
    }
	
    @Override
	protected String getDefault(){
		return "default";
	}
	
    public int getMaxTableChars() {
        return 30;
    }

    @Override
    public boolean useSQLPagination(){
		return false;
	}
	
    @Override
	public String secondsdiff(EDml dmlType, Function function) {
		Object[] o = function.getMembers();
		return String.format("TIMESTAMPDIFF (2, char(%s)) + 1", rolloverParameter(dmlType, new Object[]{o[1], o[0]}, " - "));
	}
    
    @Override
	public String paginate(Query query, String sql){
		if(query.getSkip() > 0) { // se o primeiro resultado esta definido o ultimo tb esta
//    		return String.format("SELECT * FROM (SELECT rr.*, ROW_NUMBER() OVER() AS rn FROM (%s FETCH FIRST %s ROWS ONLY) AS rr) AS r WHERE rn >= %s ORDER BY rn", 
//    				sql, (query.getFirstResult() + query.getMaxResults() - 1), query.getFirstResult());
			query.setParameter(Query.FIRST_RESULT, query.getSkip() + 1);
			query.setParameter(Query.LAST_RESULT, query.getSkip() + query.getLimit());
    		return String.format("SELECT * FROM (SELECT rr.*, ROW_NUMBER() OVER() AS rn FROM (%s FETCH FIRST :%s ROWS ONLY) AS rr) AS r WHERE rn >= :%s ORDER BY rn", 
    				sql, Query.LAST_RESULT, Query.FIRST_RESULT);
		} else if(query.getLimit() > 0){
//	    	return String.format("%s fetch first %s rows only", sql, query.getMaxResults());	    
			query.setParameter(Query.LAST_RESULT, query.getLimit());
	    	return String.format("%s fetch first :%s rows only", sql, Query.LAST_RESULT);	    
    	} else
    		return sql;
	}
}
