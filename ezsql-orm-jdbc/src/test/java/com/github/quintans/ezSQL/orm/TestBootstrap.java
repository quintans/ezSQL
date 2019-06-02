package com.github.quintans.ezSQL.orm;

import com.github.quintans.ezSQL.AbstractDb;
import com.github.quintans.ezSQL.TransactionManager;
import com.github.quintans.ezSQL.dml.Insert;
import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.orm.app.domain.EGender;
import com.github.quintans.ezSQL.orm.app.mappings.TArtist;
import com.github.quintans.ezSQL.orm.app.mappings.TEmployee;
import com.github.quintans.ezSQL.orm.app.mappings.TGallery;
import com.github.quintans.ezSQL.orm.app.mappings.TImage;
import com.github.quintans.ezSQL.orm.app.mappings.TPainting;
import com.github.quintans.ezSQL.orm.app.mappings.TTemporal;
import com.github.quintans.ezSQL.orm.app.mappings.discriminator.TBe;
import com.github.quintans.ezSQL.orm.app.mappings.discriminator.TCe;
import com.github.quintans.ezSQL.orm.app.mappings.discriminator.TMain;
import com.github.quintans.ezSQL.orm.app.mappings.virtual.TBook;
import com.github.quintans.ezSQL.orm.app.mappings.virtual.TBook18;
import com.github.quintans.ezSQL.toolkit.io.BinStore;
import com.github.quintans.ezSQL.translator.Translator;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import static com.github.quintans.ezSQL.orm.app.mappings.virtual.TAuthor.T_AUTHOR;
import static com.github.quintans.ezSQL.orm.app.mappings.virtual.TCatalog.T_CATALOG;

/**
 * Unit test for simple App.
 */
public class TestBootstrap {
    private static Logger LOGGER = Logger.getLogger(TestBootstrap.class);
    public static final String RESOURCE_IMAGES = "./src/test/resources/images";

    protected static TransactionManager<Db> tm;
    protected static Driver driver;
    protected static Translator translator;
    private static String environment;

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Iterable<Object[]> connections() {
        return Arrays.asList(new Object[][]{
                {"h2"},
                {"mysql"},
                {"postgresql"}
        });
    }

    public TestBootstrap(String environment) {
        if (environment.equals(this.environment)) {
            return;
        }

        this.environment = environment;
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(environment + ".properties")) {
            Properties systemProps = new Properties();
            systemProps.load(is);
            String dbDriver = systemProps.getProperty("db.translator");
            String dbUrl = systemProps.getProperty("db.url");
            String ormTranslator = systemProps.getProperty("db.orm.translator");
            String ormDriver = systemProps.getProperty("db.orm.driver");

            Class<?> clazz = Class.forName(ormDriver);
            driver = (Driver) clazz.newInstance();

            clazz = Class.forName(ormTranslator);
            translator = (Translator) clazz.newInstance();

            // register translator
            Class.forName(dbDriver);
            // get connection
            Connection conn = DriverManager.getConnection(dbUrl);

            tm = new TransactionManager<Db>(
                    () -> new Db(translator, driver, conn)
            ) {
                @Override
                protected void close(Connection con) {
                    // no-op for tests
                }
            };
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Before
    public void setUp() {
        tm.transactionNoResult(this::populate);
    }

    private void populate(AbstractDb db) throws IOException {
        db.delete(TEmployee.T_EMPLOYEE).execute();
        db.delete(TTemporal.T_TEMPORAL).execute();
        db.delete(T_CATALOG).execute();
        db.delete(TBook18.T_BOOK18).execute();
        db.delete(TBook.T_BOOK).execute();
        db.delete(T_AUTHOR).execute();
        db.delete(TCe.T_CE).execute();
        db.delete(TBe.T_BE).execute();
        db.delete(TMain.T_MAIN).execute();
        db.delete(TGallery.GalleryPainting.T_GALLERY_PAINTING).execute();
        db.delete(TGallery.T_GALLERY).execute();
        db.delete(TPainting.T_PAINTING).execute();
        db.delete(TImage.T_IMAGE).execute();
        db.delete(TArtist.T_ARTIST).execute();

        Insert insert = db.insert(TArtist.T_ARTIST)
                .sets(TArtist.C_ID, TArtist.C_VERSION, TArtist.C_NAME, TArtist.C_GENDER);
        insert.values(1L, 1, "Pablo Picasso", EGender.MALE).execute();
        insert.values(2L, 1, "Vincent van Gogh", EGender.MALE).execute();
        insert.values(3L, 1, "Jane Doe", EGender.FEMALE).execute();

        insert = db.insert(TImage.T_IMAGE)
                .sets(TImage.C_ID, TImage.C_VERSION, TImage.C_CONTENT);
        insert.values(1L, 1, BinStore.ofFile(RESOURCE_IMAGES + "/Blue_Nude.jpg")).execute();
        insert.values(2L, 1, BinStore.ofFile(RESOURCE_IMAGES + "/Girl_Before_a_Mirror.jpg")).execute();
        insert.values(3L, 1, BinStore.ofFile(RESOURCE_IMAGES + "/Starry_Night.jpg")).execute();
        insert.values(4L, 1, BinStore.ofFile(RESOURCE_IMAGES + "/Wheat_Field_with_Cypresses.jpg")).execute();

        insert = db.insert(TPainting.T_PAINTING)
                .sets(TPainting.C_ID, TPainting.C_VERSION, TPainting.C_NAME, TPainting.C_PRICE, TPainting.C_ARTIST, TPainting.C_IMAGE);
        insert.values(1L, 1, "Blue Nude", 23.45D, 1L, 1L).execute();
        insert.values(2L, 1, "Girl Before a Mirror", 100.00D, 1L, 2L).execute();
        insert.values(3L, 1, "The Starry Night", 356.78D, 2L, 3L).execute();
        insert.values(4L, 1, "Wheat Field with Cypresses", 100.00D, 2L, 4L).execute();

        insert = db.insert(TGallery.T_GALLERY)
                .sets(TGallery.C_ID, TGallery.C_VERSION, TGallery.C_NAME, TGallery.C_ADRESS);
        insert.values(1L, 1, "Galeria VERDE", "Rua das Alfaces 145, 1000 LISBOA").execute();
        insert.values(2L, 1, "Galeria AZUL", "Rua dos Bimbos 69, 4000 PORTO").execute();

        insert = db.insert(TGallery.GalleryPainting.T_GALLERY_PAINTING)
                .sets(TGallery.GalleryPainting.C_GALLERY, TGallery.GalleryPainting.C_PAINTING);
        insert.values(1L, 1L).execute();
        insert.values(1L, 2L).execute();
        insert.values(1L, 3L).execute();
        insert.values(2L, 1L).execute();
        insert.values(2L, 2L).execute();

        insert = db.insert(TMain.T_MAIN)
                .sets(TMain.C_ID, TMain.C_TYPE);
        insert.values(1L, "B").execute();
        insert.values(2L, "B").execute();
        insert.values(3L, "C").execute();

        insert = db.insert(TBe.T_BE)
                .sets(TBe.C_ID, TBe.C_DSC, TBe.C_FK);
        insert.values(1L, "Tequila", 1L).execute();
        insert.values(2L, "Gin", 2L).execute();
        insert.values(3L, "Vodka", null).execute();

        insert = db.insert(TCe.T_CE)
                .sets(TCe.C_ID, TCe.C_DSC, TCe.C_FK);
        insert.values(1L, "Green", 1L).execute();
        insert.values(2L, "Blue", 3L).execute();

        insert = db.insert(T_AUTHOR)
                .sets(T_AUTHOR.C_ID, T_AUTHOR.C_VERSION, T_AUTHOR.C_NAME);
        insert.values(1L, 1, "Ambrósio").execute();
        insert.values(2L, 1, "Kamon").execute();

        insert = db.insert(TBook.T_BOOK)
                .sets(TBook.C_ID, TBook.C_VERSION, TBook.C_AUTHOR, TBook.C_PRICE);
        insert.values(1L, 1, 1, 10.00D).execute();
        insert.values(2L, 1, 1, 15.00D).execute();

        insert = db.insert(TBook18.T_BOOK18)
                .sets(TBook18.C_ID, TBook18.C_LANG, TBook18.C_NAME);
        insert.values(1L, "en", "SQL in Action").execute();
        insert.values(1L, "pt", "SQL em Acção").execute();
        insert.values(2L, "en", "Twilight").execute();
        insert.values(2L, "pt", "Crepusculo").execute();

        insert = db.insert(T_CATALOG)
                .sets(T_CATALOG.C_ID, T_CATALOG.C_TYPE, T_CATALOG.C_KEY, T_CATALOG.C_VALUE);
        insert.values(1L, "EYECOLOR", "BLUE", "Blue").execute();
        insert.values(2L, "EYECOLOR", "GREEN", "Green").execute();
        insert.values(3L, "EYECOLOR", "BROWN", "Brown").execute();
        insert.values(4L, "GENDER", "M", "Male").execute();
        insert.values(5L, "GENDER", "F", "Female").execute();
        insert.values(6L, "GENDER", "U", "Unknown").execute();

    }

    @AfterClass
    public static void shutDown() {
        environment = null;
    }

    public void dumpCollection(Collection<?> collection) {
        if (!LOGGER.isDebugEnabled()) {
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
        if (!LOGGER.isDebugEnabled()) {
            return;
        }

        System.out.println(o == null ? "NULL" : o.toString());
    }

    public void dumpRaw(Collection<Object[]> collection) {
        if (!LOGGER.isDebugEnabled()) {
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
        if (!LOGGER.isDebugEnabled()) {
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
