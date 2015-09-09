package pt.quintans.ezSQL.orm.app.mappings;

import java.util.Date;

import pt.quintans.ezSQL.db.*;
import pt.quintans.ezSQL.orm.app.domain.EGender;

public class TArtist {
    public static final Table T_ARTIST = new Table("ARTIST");

    public static final Column<Long> C_ID = T_ARTIST.BIGINT("ID").key();
    public static final Column<Integer> C_VERSION = T_ARTIST.INTEGER("VERSION").version();
    public static final Column<String> C_NAME = T_ARTIST.VARCHAR("NAME");
    public static final Column<EGender> C_GENDER = T_ARTIST.NAMED("GENDER");
    public static final Column<Date> C_BIRTHDAY = T_ARTIST.TIMESTAMP("BIRTHDAY");

    // ONE artist has MANY paintings
    public final static Association A_PAINTINGS = T_ARTIST
            .ASSOCIATE(C_ID).TO(TPainting.C_ARTIST).AS("paintings");
}
