package com.github.quintans.ezSQL.driver;

import com.github.quintans.ezSQL.dml.AutoKeyStrategy;
import com.github.quintans.ezSQL.dml.Query;
import com.github.quintans.ezSQL.transformers.Record;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;

import java.sql.Connection;
import java.util.Calendar;

public interface Driver {
  void setTimeZoneId(String tzId);

  Calendar getCalendar();

  AutoKeyStrategy getAutoKeyStrategy();

  void prepareConnection(Connection connection);

  boolean isPmdKnownBroken();

  Object toDb(Object parameter);

  Record newRecord(Query query, ResultSetWrapper rsw);

}
