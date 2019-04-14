package com.github.quintans.ezSQL.orm.app.daos;

import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.orm.app.domain.Employee;
import com.github.quintans.ezSQL.orm.app.mappings.TEmployee;
import com.github.quintans.ezSQL.transformers.MapColumn;
import com.github.quintans.ezSQL.transformers.QueryMapper;
import com.github.quintans.ezSQL.transformers.Record;
import com.github.quintans.jdbc.exceptions.PersistenceException;

import java.util.Date;
import java.util.List;

public class EmployeeDAOTransformer implements QueryMapper<Employee> {
    private Driver driver;

    public EmployeeDAOTransformer(Driver driver) {
        this.driver = driver;
    }

    @Override
    public Object createFrom(Object instance, String name) {
        return new Employee();
    }

    @Override
    public void link(Object instance, String name, Object value) {

    }

    @Override
    public boolean map(Record record, Object instance, List<MapColumn> mapColumns) {
        try {
            boolean touched = false;

            Employee entity = (Employee) instance;

            for (MapColumn mapColumn : mapColumns) {
                int idx = mapColumn.getIndex();
                String alias = mapColumn.getAlias();

                if (TEmployee.C_ID.getAlias().equals(alias)) {
                    Long value = record.getLong(idx);
                    entity.setId(value);
                    touched |= value != null;
                } else if (TEmployee.C_NAME.getAlias().equals(alias)) {
                    String value = record.getString(idx);
                    entity.setName(value);
                    touched |= value != null;
                } else if (TEmployee.C_SEX.getAlias().equals(alias)) {
                    Boolean value = record.getBoolean(idx);
                    entity.setSex(value);
                    touched |= value != null;
                } else if (TEmployee.C_CREATION.getAlias().equals(alias)) {
                    Date value = record.getDate(idx);
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
