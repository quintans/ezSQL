package com.github.quintans.ezSQL.orm;

import com.github.quintans.ezSQL.driver.Driver;
import org.apache.log4j.Logger;
import org.dbunit.IDatabaseTester;
import org.dbunit.JdbcDatabaseTester;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.XmlDataSet;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.util.Collection;
import java.util.Properties;

/**
 * Unit test for simple App.
 */
public class TestBootstrap {
    private static Logger LOGGER = Logger.getLogger(TestBootstrap.class);

    private static IDatabaseTester databaseTester;
    protected static Db db;

    @BeforeClass
    public static void testSetup() throws Exception {
        try {
            final String env = System.getProperty("env");
            Properties systemProps = new Properties();
            systemProps.load(new FileReader(new File("src/test/resources/" + env + ".properties")));
            String dbDriver = systemProps.getProperty("db.driver");
            String dbUrl = systemProps.getProperty("db.url");
            String dbUser = systemProps.getProperty("db.user");
            String dbPassword = systemProps.getProperty("db.password");
            String ormDriver = systemProps.getProperty("db.orm.driver");

            databaseTester = new JdbcDatabaseTester(dbDriver, dbUrl, dbUser, dbPassword);

            //databaseTester = new JdbcDatabaseTester("org.h2.Driver", "jdbc:h2:tcp://localhost:9092/test", "sa", "");
            //databaseTester = new JdbcDatabaseTester("org.mariadb.jdbc.Driver", "jdbc:mariadb://localhost:3306/ezsql", "quintans", "quintans");
            //databaseTester = new JdbcDatabaseTester("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/ezsql", "quintans", "quintans");
            // databaseTester = new JdbcDatabaseTester("org.postgresql.Driver", "jdbc:postgresql://localhost:5432/ezsql", "quintans", "quintans");

            db = new Db(databaseTester.getConnection().getConnection());

            Class<?> clazz = Class.forName(ormDriver);
            Driver driver = (Driver) clazz.newInstance();

            //Driver driver = new MySQLDriverExt();
            // Driver driver = new H2DriverExt();
            //Driver driver = new Oracle8iDriver();
            //Driver driver = new PostgreSQLDriverExt();

            db.setDriver(driver);

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Before
    public void setUp() throws Exception {
        try {
            // initialize your dataset here
            IDataSet dataSet = new XmlDataSet(new FileInputStream("data/export.xml"));

            databaseTester.setDataSet(dataSet);
            // will call default setUpOperation
            databaseTester.onSetup();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @After
    public void tearDown() throws Exception {
        databaseTester.onTearDown();
    }

	/*
    @BeforeClass
    public static void testSetup() throws Exception {
        try {
            String dbms = System.getProperty("dbms");
            Connection conn = null;
            if (dbms == null || dbms.equals("mysql")) {
                conn = DriverManager.getConnection("jdbc:mariadb://localhost:3306/ezsql", "quintans", "quintans");
            } else if (dbms.equals("h2")) {
                conn = DriverManager.getConnection("jdbc:h2:tcp://localhost:9092/test", "sa", "");
            }
            
            db = new Db(conn);
            Driver driver = new H2DriverExt();
            // Driver driver = new Oracle8iDriver();
            db.setDriver(driver);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Before
    public void setUp() throws Exception {
        try {
            // DELETES
            db.delete(TArtist.T_ARTIST).execute();
            
            Insert insert = db.insert(TArtist.T_ARTIST)
            .sets(TArtist.C_ID, TArtist.C_VERSION, TArtist.C_GENDER, TArtist.C_NAME, TArtist.C_BIRTHDAY);

            insert.values(1, 1, EGender.MALE, "Pablo Picasso", null).execute();
            insert.values(2, 1, EGender.MALE, "Vincent van Gogh", null).execute();
            insert.values(3, 1, EGender.FEMALE, "Jane Doe", null).execute();
            
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
    
    @After
    public void tearDown() throws Exception {
        db.getConnection().commit();
    }
    */

    public void dumpCollection(Collection<?> collection) {
        if(!LOGGER.isDebugEnabled()) {
            return;
        }

        System.out.println("============================================================>");
        if (collection == null || collection.isEmpty())
            System.out.println("NO DATA FOUND");
        else {
            for (Object o : collection)
                dump(o);
        }
    }

    public void dump(Object o) {
        if(!LOGGER.isDebugEnabled()) {
            return;
        }

        System.out.println(o == null ? "NULL" : o.toString());
    }

    public void dumpRaw(Collection<Object[]> collection) {
        if(!LOGGER.isDebugEnabled()) {
            return;
        }

        if (collection.isEmpty())
            System.out.println("NO DATA FOUND");
        else {
            int[] sizes = new int[collection.iterator().next().length];
            for (Object[] objs : collection)
                calc(objs, sizes);
            StringBuilder sb = new StringBuilder();
            for (int sz : sizes) {
                for (int i = 0; i < sz + 3; i++) {
                    sb.append("=");
                }
            }
            System.out.println(sb.toString());
            for (Object[] objs : collection)
                dumpRaw(objs, sizes);
        }
    }

    public void dumpRaw(Object[] objs, int[] sizes) {
        if(!LOGGER.isDebugEnabled()) {
            return;
        }

        if (objs == null)
            System.out.println("NULL");
        else {
            for (int i = 0; i < objs.length; i++)
                System.out.print(" " + rpad(objs[i], sizes[i]) + " |");
            System.out.println();
        }
    }

    private void calc(Object[] objs, int[] sizes) {
        if (objs != null) {
            for (int i = 0; i < objs.length; i++) {
                sizes[i] = Math.max(objs[i] == null ? 4 : objs[i].toString().length(), sizes[i]);
            }
        }
    }

    public String rpad(Object o, int size) {
        String str = null;
        if (o == null)
            str = "null";
        else
            str = o.toString();

        if (str.length() < size) {
            StringBuilder sb = new StringBuilder(str);
            for (int i = str.length(); i < size; i++) {
                sb.append(" ");
            }
            return sb.toString();
        } else
            return str;
    }

    //============================ TESTS =====================================
}
