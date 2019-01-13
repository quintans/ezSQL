package com.github.quintans.ezSQL.orm;

import java.sql.SQLException;
import java.util.Date;
import java.util.Random;

import com.github.quintans.ezSQL.transformers.SimpleAbstractDbRowTransformer;
import com.github.quintans.jdbc.RawSql;
import com.github.quintans.jdbc.SimpleJdbc;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;
import com.github.quintans.jdbc.transformers.SimpleAbstractRowTransformer;

import org.junit.Test;

import com.github.quintans.ezSQL.DbJdbcSession;
import com.github.quintans.ezSQL.dml.Insert;
import com.github.quintans.ezSQL.dml.Query;
import com.github.quintans.ezSQL.orm.app.daos.EmployeeDAOBase;
import com.github.quintans.ezSQL.orm.app.domain.Employee;
import com.github.quintans.ezSQL.orm.app.mappings.TEmployee;

/**
 * Unit test for simple App.
 */
public class TestPerformance extends TestBootstrap {

    // private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    private static final String femaleFirstNames[] = { "Maria", "Joana", "Mónica", "Paula", "Rosa", "Carla", "Claudia" };
    private static final String maleFirstNames[] = { "Mario", "João", "Marco", "Paulo", "Rui", "Carlos", "Raul" };
    private static final String lastNames[] = { "Marques", "Fernandes", "Machado", "Pereira", "Rodrigues", "Campos", "Lourosa" };
    private static final long YEAR = 365L * 24L * 3600000L;

    private static final int LOOP = 10000;
    private static final int BATCH = 1000;

    @Test
    public void testInsertDirectOrByReflectian() throws Exception {
        try {
            Insert insert;
            Stopwatch sw = new Stopwatch();
            Random rnd = new Random();
            boolean sex;
            
            
            db.delete(TEmployee.T_EMPLOYEE).execute();
            // INSERT
            sex = rnd.nextBoolean();
            // the insert object is created just to get an database agnostic sql
            insert = db.insert(TEmployee.T_EMPLOYEE)
                    .sets(TEmployee.C_ID, TEmployee.C_NAME, TEmployee.C_SEX, TEmployee.C_CREATION)
                    .set(TEmployee.C_ID, 0L); // Force the ID placeholder generation for Postgresql. The Postgresql Driver does not generate ? for null IDs.
            SimpleJdbc jdbc = insert.getSimpleJdbc();
            RawSql cachedSql = insert.getSql();

            for (int i = 1; i <= LOOP; i++) {
                int firstNameIdx = rnd.nextInt(7);
                int lastNameIdx = rnd.nextInt(7);
                String name = (sex ? maleFirstNames[firstNameIdx] : femaleFirstNames[firstNameIdx]) + " " + lastNames[lastNameIdx];
                Date creation = new Date(System.currentTimeMillis() - (rnd.nextInt(40) * YEAR));

                sw.start();
                jdbc.insert(cachedSql.getSql(), null, db.transformParameters(i, name, sex, creation));
                sw.stop();
                sex = !sex;
            }
            sw.showAverage("DIRECT");

            db.delete(TEmployee.T_EMPLOYEE).execute();
            sw.reset();
            jdbc = new SimpleJdbc(new DbJdbcSession(db));
            String sql = cachedSql.getSql();
            sex = rnd.nextBoolean();
            for (int i = 1; i <= LOOP; i++) {
                int firstNameIdx = rnd.nextInt(7);
                int lastNameIdx = rnd.nextInt(7);
                String name = (sex ? maleFirstNames[firstNameIdx] : femaleFirstNames[firstNameIdx]) + " " + lastNames[lastNameIdx];
                Date creation = new Date(System.currentTimeMillis() - (rnd.nextInt(40) * YEAR));

                sw.start();
                jdbc.batch(sql, db.transformParameters(i, name, sex, creation));
                if(i % BATCH == 0) {
                    jdbc.flushInsert();
                }
                sw.stop();
                sex = !sex;
            }
            if(jdbc.getPending()>0) {
                sw.start();
                jdbc.flushInsert();
                sw.stop();
            }
            sw.showAverage("BATCH");
            
            
            db.delete(TEmployee.T_EMPLOYEE).execute();
            sw.reset();
            insert = db.insert(TEmployee.T_EMPLOYEE).retriveKeys(false)
                    .sets(TEmployee.C_ID, TEmployee.C_NAME, TEmployee.C_SEX, TEmployee.C_CREATION);
            sex = rnd.nextBoolean();
            for (int i = 1; i <= LOOP; i++) {
                int firstNameIdx = rnd.nextInt(7);
                int lastNameIdx = rnd.nextInt(7);
                String name = (sex ? maleFirstNames[firstNameIdx] : femaleFirstNames[firstNameIdx]) + " " + lastNames[lastNameIdx];
                Date birth = new Date(System.currentTimeMillis() - (rnd.nextInt(40) * YEAR));

                sw.start();
                insert.values(i, name, sex, birth).batch();
                if(i % BATCH == 0) {
                    insert.flushBatch();
                }
                sw.stop();
                sex = !sex;
            }
            if(insert.getPending()>0) {
                sw.start();
                insert.endBatch();
                sw.stop();
            }
            sw.showAverage("BATCH INCLUDED");
            
            
            db.delete(TEmployee.T_EMPLOYEE).execute();
            sw.reset();
            // INSERT
            insert = db.insert(TEmployee.T_EMPLOYEE).retriveKeys(false)
                    .sets(TEmployee.C_ID, TEmployee.C_NAME, TEmployee.C_SEX, TEmployee.C_CREATION);

            sex = rnd.nextBoolean();
            sw.reset();
            for (int i = 1; i <= LOOP; i++) {
                int firstNameIdx = rnd.nextInt(7);
                int lastNameIdx = rnd.nextInt(7);
                String name = (sex ? maleFirstNames[firstNameIdx] : femaleFirstNames[firstNameIdx]) + " " + lastNames[lastNameIdx];
                Date birth = new Date(System.currentTimeMillis() - (rnd.nextInt(40) * YEAR));

                sw.start();
                insert.values(i, name, sex, birth).execute();
                sw.stop();
                sex = !sex;
            }
            sw.showAverage("SETS");

            db.delete(TEmployee.T_EMPLOYEE).execute();
            sw.reset();
            // INSERT
            insert = db.insert(TEmployee.T_EMPLOYEE);

            rnd = new Random();
            sex = rnd.nextBoolean();
            sw.reset();
            for (long i = 1; i <= LOOP; i++) {
                int firstNameIdx = rnd.nextInt(7);
                int lastNameIdx = rnd.nextInt(7);
                String name = (sex ? maleFirstNames[firstNameIdx] : femaleFirstNames[firstNameIdx]) + " " + lastNames[lastNameIdx];
                Date birth = new Date(System.currentTimeMillis() - (rnd.nextInt(40) * YEAR));

                Employee employee = new Employee();
                employee.setId(i);
                employee.setName(name);
                employee.setSex(sex);
                employee.setCreation(birth);
                sw.start();
                insert.set(employee).execute();
                sw.stop();

                sex = !sex;
            }
            sw.showAverage("BEANS");
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void testQueryByReflectianAndByTransformer() {

        db.delete(TEmployee.T_EMPLOYEE).execute();

        Insert insert = new Insert(db, TEmployee.T_EMPLOYEE)
                .sets(TEmployee.C_ID, TEmployee.C_NAME, TEmployee.C_SEX, TEmployee.C_CREATION);

        Random rnd = new Random();
        boolean sex = rnd.nextBoolean();
        for (int i = 1; i <= LOOP; i++) {
            int firstNameIdx = rnd.nextInt(7);
            int lastNameIdx = rnd.nextInt(7);
            String name = (sex ? maleFirstNames[firstNameIdx] : femaleFirstNames[firstNameIdx]) + " " + lastNames[lastNameIdx];
            Date birth = new Date(System.currentTimeMillis() - (rnd.nextInt(40) * YEAR));

            insert.values(i, name, sex, birth).execute();
            sex = !sex;
        }

        Stopwatch sw = new Stopwatch();
        Query query = null;
        // warm up
        query = db.query(TEmployee.T_EMPLOYEE).all();
        query.list(EmployeeDAOBase.factory);
        // READ - ORM transformer
        query = db.query(TEmployee.T_EMPLOYEE).all();
        sw.reset().start();
        query.list(EmployeeDAOBase.factory);
        sw.stop().showTotal("query.list(EmployeeDAOBase.factory)");

        // warm up
        query = db.query(TEmployee.T_EMPLOYEE).all();
        query.list(new SimpleAbstractRowTransformer<Employee>() {
            @Override
            public Employee transform(ResultSetWrapper rs) throws SQLException {
                return new Employee();
            }
        });
        // READ - transformer
        query = db.query(TEmployee.T_EMPLOYEE).all();
        sw.reset().start();
        query.list(new SimpleAbstractDbRowTransformer<Employee>(db) {
            @Override
            public Employee transform(ResultSetWrapper rsw) throws SQLException {
                Employee dto = new Employee();
                dto.setId(toLong(rsw, 1));
                dto.setName(toString(rsw, 2));
                dto.setSex(toBoolean(rsw, 3));
                dto.setCreation(toDate(rsw, 4));
                return dto;
            }
        });
        sw.stop().showTotal("query.list(new SimpleAbstractDbRowTransformer<Employee>(db))");

        // warm up
        query = db.query(TEmployee.T_EMPLOYEE).all();
        query.list(Employee.class);
        // READ - Reflection
        query = db.query(TEmployee.T_EMPLOYEE).all();
        sw.reset().start();
        query.list(Employee.class, false);
        sw.stop().showTotal("query.list(Employee.class)");

    }
}
