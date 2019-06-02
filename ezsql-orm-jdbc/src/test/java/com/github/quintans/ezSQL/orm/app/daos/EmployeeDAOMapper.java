package com.github.quintans.ezSQL.orm.app.daos;

import com.github.quintans.ezSQL.orm.app.domain.Employee;
import com.github.quintans.ezSQL.orm.app.mappings.TEmployee;
import com.github.quintans.ezSQL.mapper.MapColumn;
import com.github.quintans.ezSQL.mapper.QueryMapper;
import com.github.quintans.jdbc.exceptions.PersistenceException;
import com.github.quintans.ezSQL.mapper.Row;

import java.util.Date;
import java.util.List;

public class EmployeeDAOMapper implements QueryMapper {

  @Override
  public boolean support(Class<?> rootClass) {
    return Employee.class.equals(rootClass);
  }

  @Override
  public Object createRoot(Class<?> rootClass) {
    return new Employee();
  }

  @Override
  public Object createFrom(Object instance, String name) {
    throw new IllegalArgumentException("Unknown mapping for alias " +
        instance.getClass().getCanonicalName() + "#" + name);
  }

  @Override
  public void link(Object instance, String name, Object value) {

  }

  @Override
  public boolean map(Row row, Object instance, List<MapColumn> mapColumns) {
    try {
      boolean touched = false;

      Employee entity = (Employee) instance;

      for (MapColumn mapColumn : mapColumns) {
        int idx = mapColumn.getIndex();
        String alias = mapColumn.getAlias();

        if (TEmployee.C_ID.getAlias().equals(alias)) {
          Long value = row.get(idx, Long.class);
          entity.setId(value);
          touched |= value != null;
        } else if (TEmployee.C_NAME.getAlias().equals(alias)) {
          String value = row.get(idx, String.class);
          entity.setName(value);
          touched |= value != null;
        } else if (TEmployee.C_SEX.getAlias().equals(alias)) {
          Boolean value = row.get(idx, Boolean.class);
          entity.setSex(value);
          touched |= value != null;
        } else if (TEmployee.C_CREATION.getAlias().equals(alias)) {
          Date value = row.get(idx, Date.class);
          entity.setCreation(value);
          touched |= value != null;
        }
      }

      return touched;

    } catch (Exception e) {
      throw new PersistenceException(e);
    }

  }
}
