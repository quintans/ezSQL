package com.github.quintans.ezSQL.orm;

import com.github.quintans.ezSQL.AbstractDb;
import com.github.quintans.ezSQL.orm.app.domain.Employee;
import com.github.quintans.ezSQL.orm.app.mappings.TEmployee;
import org.junit.Test;

import java.awt.*;

import static org.junit.Assert.assertEquals;

public class TestConverter extends TestBootstrap {

    private final Color BEIGE = new Color(102, 102, 0);

    @Test
    public void testInsert() {
        tm.transactionNoResult(db -> {
            Employee employee = new Employee();
            employee.setName("Paulo");
            employee.setEyeColor(new Color(102, 102, 102));

            db.insert(TEmployee.T_EMPLOYEE).submit(employee);
            String eyeColor = getEyeColor(db, employee.getId());
            assertEquals("Wrong value for eye color", "102|102|102|255", eyeColor);

            employee.setEyeColor(BEIGE);
            db.update(TEmployee.T_EMPLOYEE).submit(employee);
            eyeColor = getEyeColor(db, employee.getId());
            assertEquals("Wrong value for eye color", "102|102|0|255", eyeColor);

            employee = db.query(TEmployee.T_EMPLOYEE)
                    .where(TEmployee.C_ID.is(employee.getId()))
                    .unique(Employee.class);
            assertEquals("Wrong value for eye color", BEIGE, employee.getEyeColor());
        });
    }

    private String getEyeColor(AbstractDb db, Long id) {
        return db.query(TEmployee.T_EMPLOYEE).column(TEmployee.C_EYE_COLOR)
                .where(TEmployee.C_ID.is(id))
                .uniqueString();
    }
}
