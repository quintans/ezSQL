package com.github.quintans.ezSQL.db;

import com.github.quintans.ezSQL.AbstractDb;
import com.github.quintans.jdbc.SimpleJdbc;
import org.apache.log4j.Logger;

import java.util.LinkedHashMap;

public class Sequence extends SequenceDSL {
  private static Logger LOGGER = Logger.getLogger(Sequence.class);

  private AbstractDb db;
  protected SimpleJdbc simpleJdbc;

  private String name;

  public Sequence(AbstractDb db, String name) {
    super(name);
    this.db = db;
    this.simpleJdbc = new SimpleJdbc(db.getJdbcSession());
  }

  @Override
  public Long fetchSequence(boolean nextValue) {
    String sql = this.db.getTranslator().getSql(this, nextValue);
    long now = 0;
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("SQL: " + sql);
      now = System.nanoTime();
    }
    Long id = simpleJdbc.queryForLong(sql, new LinkedHashMap<String, Object>());
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("executed in: " + (System.nanoTime() - now) / 1e6 + "ms");
    }
    return id;
  }

}
