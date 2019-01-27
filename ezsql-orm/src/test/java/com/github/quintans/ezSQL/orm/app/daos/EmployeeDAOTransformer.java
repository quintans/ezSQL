package com.github.quintans.ezSQL.orm.app.daos;

import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.orm.app.domain.Employee;
import com.github.quintans.ezSQL.orm.app.mappings.TEmployee;
import com.github.quintans.ezSQL.transformers.MapColumn;
import com.github.quintans.ezSQL.transformers.Mapper;
import com.github.quintans.jdbc.exceptions.PersistenceException;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;

import java.util.Date;
import java.util.List;

public class EmployeeDAOTransformer implements Mapper {
    private Driver driver;

    public EmployeeDAOTransformer(Driver driver) {
        this.driver = driver;
    }

    @Override
    public Object createFrom(Object instance, String name) {
        return new Employee();
    }

    @Override
    public void apply(Object instance, String name, Object value) {

    }

    @Override
    public boolean map(ResultSetWrapper rsw, Object instance, List<MapColumn> mapColumns) {
        try {
            boolean touched = false;

            Employee entity = (Employee) instance;

            for (MapColumn mapColumn : mapColumns) {
                int idx = mapColumn.getColumnIndex();
                String alias = mapColumn.getAlias();

                if (TEmployee.C_ID.getAlias().equals(alias)) {
                    Long value = driver.toLong(rsw, idx);
                    entity.setId(value);
                    touched |= value != null;
                } else if (TEmployee.C_NAME.getAlias().equals(alias)) {
                    String value = driver.toString(rsw, idx);
                    entity.setName(value);
                    touched |= value != null;
                } else if (TEmployee.C_SEX.getAlias().equals(alias)) {
                    Boolean value = driver.toBoolean(rsw, idx);
                    entity.setSex(value);
                    touched |= value != null;
                } else if (TEmployee.C_CREATION.getAlias().equals(alias)) {
                    Date value = driver.toDate(rsw, idx);
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
