package com.github.quintans.ezSQL.orm;

import com.github.quintans.ezSQL.DbJdbcSession;
import com.github.quintans.ezSQL.dml.Insert;
import com.github.quintans.ezSQL.dml.JdbcExecutor;
import com.github.quintans.ezSQL.dml.Query;
import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.orm.app.daos.EmployeeDAOTransformer;
import com.github.quintans.ezSQL.orm.app.domain.Employee;
import com.github.quintans.ezSQL.orm.app.mappings.TEmployee;
import com.github.quintans.ezSQL.transformers.MapTransformer;
import com.github.quintans.jdbc.RawSql;
import com.github.quintans.jdbc.SimpleJdbc;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;
import com.github.quintans.jdbc.transformers.SimpleAbstractRowTransformer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.util.Date;
import java.util.Random;

@RunWith(Parameterized.class)
public class TestPerformance extends TestBootstrap {

    // private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    private static final String femaleFirstNames[] = {"Maria", "Joana", "Mónica", "Paula", "Rosa", "Carla", "Claudia"};
    private static final String maleFirstNames[] = {"Mario", "João", "Marco", "Paulo", "Rui", "Carlos", "Raul"};
    private static final String lastNames[] = {"Marques", "Fernandes", "Machado", "Pereira", "Rodrigues", "Campos", "Lourosa"};
    private static final long YEAR = 365L * 24L * 3600000L;

    private static final int WARM_UP = 100;
    private static final int LOOP = 10000;
    private static final int BATCH = 1000;

    public TestPerformance(String environment) {
        super(environment);
    }

    @Test
    public void testInsertDirectOrByReflectian() throws Exception {
        tm.transactionNoResult(db -> {
            Insert insert;
            Stopwatch sw = new Stopwatch();
            Random rnd = new Random();
            boolean sex;


            db.delete(TEmployee.T_EMPLOYEE).execute();
            // INSERT
            sex = rnd.nextBoolean();
            // the insert object is created just to getConnection an database agnostic sql
            insert = db.insert(TEmployee.T_EMPLOYEE)
                    .sets(TEmployee.C_ID, TEmployee.C_NAME, TEmployee.C_SEX, TEmployee.C_CREATION)
                    .set(TEmployee.C_ID, 0L); // Force the ID placeholder generation for Postgresql. The Postgresql Driver does not generate ? for null IDs.
            SimpleJdbc jdbc = new SimpleJdbc(db.getJdbcSession());
            JdbcExecutor executor = new JdbcExecutor(driver, jdbc);
            RawSql cachedSql = insert.getRawSql();

            for (int i = 1; i <= LOOP; i++) {
                int firstNameIdx = rnd.nextInt(7);
                int lastNameIdx = rnd.nextInt(7);
                String name = (sex ? maleFirstNames[firstNameIdx] : femaleFirstNames[firstNameIdx]) + " " + lastNames[lastNameIdx];
                Date creation = new Date(System.currentTimeMillis() - (rnd.nextInt(40) * YEAR));

                sw.start();
                jdbc.insert(cachedSql.getSql(), null, executor.transformParameters(i, name, sex, creation));
                sw.stop();
                sex = !sex;
            }
            sw.showAverage("DIRECT");

            db.delete(TEmployee.T_EMPLOYEE).execute();
            sw.reset();
            jdbc = new SimpleJdbc(new DbJdbcSession(db, driver.isPmdKnownBroken()));
            String sql = cachedSql.getSql();
            sex = rnd.nextBoolean();
            for (int i = 1; i <= LOOP; i++) {
                int firstNameIdx = rnd.nextInt(7);
                int lastNameIdx = rnd.nextInt(7);
                String name = (sex ? maleFirstNames[firstNameIdx] : femaleFirstNames[firstNameIdx]) + " " + lastNames[lastNameIdx];
                Date creation = new Date(System.currentTimeMillis() - (rnd.nextInt(40) * YEAR));

                sw.start();
                jdbc.batch(sql, executor.transformParameters(i, name, sex, creation));
                if (i % BATCH == 0) {
                    jdbc.flushInsert();
                }
                sw.stop();
                sex = !sex;
            }
            if (jdbc.getPending() > 0) {
                sw.start();
                jdbc.flushInsert();
                sw.stop();
            }
            sw.showAverage("BATCH");


            db.delete(TEmployee.T_EMPLOYEE).execute();
            sw.reset();
            insert = db.insert(TEmployee.T_EMPLOYEE).retrieveKeys(false)
                    .sets(TEmployee.C_ID, TEmployee.C_NAME, TEmployee.C_SEX, TEmployee.C_CREATION);
            sex = rnd.nextBoolean();
            for (int i = 1; i <= LOOP; i++) {
                int firstNameIdx = rnd.nextInt(7);
                int lastNameIdx = rnd.nextInt(7);
                String name = (sex ? maleFirstNames[firstNameIdx] : femaleFirstNames[firstNameIdx]) + " " + lastNames[lastNameIdx];
                Date birth = new Date(System.currentTimeMillis() - (rnd.nextInt(40) * YEAR));

                sw.start();
                insert.values(i, name, sex, birth).batch();
                if (i % BATCH == 0) {
                    insert.flushBatch();
                }
                sw.stop();
                sex = !sex;
            }
            if (insert.getPending() > 0) {
                sw.start();
                insert.endBatch();
                sw.stop();
            }
            sw.showAverage("BATCH INCLUDED");


            db.delete(TEmployee.T_EMPLOYEE).execute();
            sw.reset();
            // INSERT
            insert = db.insert(TEmployee.T_EMPLOYEE).retrieveKeys(false)
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
        });
    }

    @Test
    public void testQueryByReflectianAndByTransformer() {
        tm.transactionNoResult(db -> {
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

            Driver driver = db.getDriver();
            Stopwatch sw = new Stopwatch();
            Query query = null;
            // warm up
            for (int i = 0; i < WARM_UP; i++) {
                query = db.query(TEmployee.T_EMPLOYEE).all();
                query.list(new MapTransformer<>(query, false, Employee.class, new EmployeeDAOTransformer()));
            }
            // READ - ORM transformer
            query = db.query(TEmployee.T_EMPLOYEE).all();
            sw.reset().start();
            query.list(new MapTransformer<>(query, false, Employee.class, new EmployeeDAOTransformer()));
            sw.stop().showTotal("query.list(EmployeeDAOTransformer)");

            // warm up
            for (int i = 0; i < WARM_UP; i++) {
                query = db.query(TEmployee.T_EMPLOYEE).all();
                query.list(new SimpleAbstractRowTransformer<Employee>() {
                    @Override
                    public Employee transform(ResultSetWrapper rs) throws SQLException {
                        return new Employee();
                    }
                });
            }
            // READ - transformer
            query = db.query(TEmployee.T_EMPLOYEE).all();
            sw.reset().start();
            query.list(r -> {
                Employee dto = new Employee();
                dto.setId(r.getLong(1));
                dto.setName(r.getString(2));
                dto.setSex(r.getBoolean(3));
                dto.setCreation(r.getDate(4));
                return dto;
            });
            sw.stop().showTotal("query.list(new SimpleAbstractDbRowTransformer<Employee>(db))");

            // warm up
            for (int i = 0; i < WARM_UP; i++) {
                query = db.query(TEmployee.T_EMPLOYEE).all();
                query.list(Employee.class);
            }
            // READ - Reflection
            query = db.query(TEmployee.T_EMPLOYEE).all();
            sw.reset().start();
            query.list(Employee.class, false);
            sw.stop().showTotal("query.list(Employee.class)");
        });
    }
}
