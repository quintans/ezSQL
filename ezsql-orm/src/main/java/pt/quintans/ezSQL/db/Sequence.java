package pt.quintans.ezSQL.db;

import java.util.LinkedHashMap;

import org.apache.log4j.Logger;

import pt.quintans.ezSQL.AbstractDb;
import pt.quintans.ezSQL.sql.SimpleJdbc;

public class Sequence {
	private static Logger LOGGER = Logger.getLogger(Sequence.class);

	private AbstractDb db;
    protected SimpleJdbc simpleJdbc;

	private String name;

	public Sequence(AbstractDb db, String name) {
		this.db = db;
        this.simpleJdbc = new SimpleJdbc(db.getJdbcSession());
		this.name = name;
	}

	public String getName() {
		return this.name;
	}

	public Long fetchSequenceNextValue() {
		return fetchSequence(true);
	}

	public Long fetchSequenceCurrentValue() {
		return fetchSequence(false);
	}

	public Long fetchSequence(boolean nextValue) {
		String sql = this.db.getDriver().getSql(this, nextValue);
		long now = 0;
		if(LOGGER.isDebugEnabled()) {
    		LOGGER.debug("SQL: " + sql);
    		now = System.nanoTime();
		}
		Long id = simpleJdbc.queryForLong(sql, new LinkedHashMap<String, Object>());
        if(LOGGER.isDebugEnabled()) {
            LOGGER.debug("executed in: " + (System.nanoTime() - now)/1e6 + "ms");
        }
		return id;
	}

}
