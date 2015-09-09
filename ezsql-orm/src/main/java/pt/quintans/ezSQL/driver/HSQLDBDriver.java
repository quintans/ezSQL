package pt.quintans.ezSQL.driver;

import pt.quintans.ezSQL.db.Column;
import pt.quintans.ezSQL.dml.AutoKeyStrategy;
import pt.quintans.ezSQL.dml.Query;
import pt.quintans.ezSQL.exceptions.PersistenceException;


/**
 * User: quintans
 * Date: 03-dic-2007
 * Time: 15:56:18
 */
public class HSQLDBDriver extends GenericDriver {

	public String getAutoNumberQuery(Column<? extends Number> column, boolean current) {
		if(column.isKey())
			return "call identity()";
		else
			throw new PersistenceException(String.format("A função getAutoNumberQuery não reconhece a coluna %s.", column));
    }

    @Override
    public AutoKeyStrategy getAutoKeyStrategy() {
        return AutoKeyStrategy.AFTER;
    }

    public int getMaxTableChars() {
        return 30;
    }

	@Override
	public String paginate(Query query, String sql) {
		return sql;
	}

}
