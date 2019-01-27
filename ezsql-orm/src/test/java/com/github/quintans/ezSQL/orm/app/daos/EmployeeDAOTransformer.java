package com.github.quintans.ezSQL.orm.app.daos;

import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.orm.app.domain.Artist;
import com.github.quintans.ezSQL.orm.app.domain.Employee;
import com.github.quintans.ezSQL.orm.app.mappings.TArtist;
import com.github.quintans.ezSQL.orm.app.mappings.TEmployee;
import com.github.quintans.ezSQL.transformers.MapColumn;
import com.github.quintans.ezSQL.transformers.Mapper;
import com.github.quintans.jdbc.exceptions.PersistenceException;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;

import java.util.Date;

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
    public Object map(ResultSetWrapper rsw, Object instance, MapColumn mapColumn) {
        int idx = mapColumn.getColumnIndex();
        String alias = mapColumn.getAlias();

        Employee entity = (Employee) instance;

        try {
            if (TEmployee.C_ID.getAlias().equals(alias)) {
                Long value = driver.fromDb(rsw, idx, Long.class);
                entity.setId(value);
                return value;
            } else if (TEmployee.C_NAME.getAlias().equals(alias)) {
                String value = driver.fromDb(rsw, idx, String.class);
                entity.setName(value);
                return value;
            } else if (TEmployee.C_SEX.getAlias().equals(alias)) {
                Boolean value = driver.fromDb(rsw, idx, Boolean.class);
                entity.setSex(value);
                return value;
            } else if (TEmployee.C_CREATION.getAlias().equals(alias)) {
                Date value = driver.fromDb(rsw, idx, Date.class);
                entity.setCreation(value);
                return value;
            }
            return null;
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }
}
