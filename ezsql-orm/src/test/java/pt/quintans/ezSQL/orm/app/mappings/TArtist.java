package pt.quintans.ezSQL.orm.app.mappings;

import java.util.Date;

import pt.quintans.ezSQL.db.*;
import pt.quintans.ezSQL.dml.Insert;
import pt.quintans.ezSQL.dml.Update;
import pt.quintans.ezSQL.orm.app.domain.EGender;

public class TArtist {
    public static final Table T_ARTIST = new Table("ARTIST");

    public static final Column<Long> C_ID = T_ARTIST
    		.BIGINT("ID").key();
    public static final Column<Integer> C_VERSION = T_ARTIST
    		.INTEGER("VERSION").version();
    public static final Column<String> C_NAME = T_ARTIST
    		.VARCHAR("NAME");
    public static final Column<EGender> C_GENDER = T_ARTIST
    		.NAMED("GENDER");
    public static final Column<Date> C_BIRTHDAY = T_ARTIST
    		.TIMESTAMP("BIRTHDAY");
    public static final Column<Date> C_CREATION = T_ARTIST
    		.TIMESTAMP("CREATION");
    public static final Column<Date> C_MODIFICATION = T_ARTIST
    		.TIMESTAMP("MODIFICATION");
    
    // audit triggers
    static {
    	T_ARTIST.setPreInsertTrigger(new PreInsertTrigger() {
			@Override
			public void trigger(Insert ins) {
	            ins.set(C_VERSION, 1);
	            ins.set(C_CREATION, new java.util.Date());				
			}
		});
    			
    	T_ARTIST.setPreUpdateTrigger(new PreUpdateTrigger() {
			@Override
			public void trigger(Update upd) {
	            upd.set(C_MODIFICATION, new java.util.Date());
			}
		});
    }

    // ONE artist has MANY paintings
    public final static Association A_PAINTINGS = T_ARTIST
            .ASSOCIATE(C_ID).TO(TPainting.C_ARTIST).AS("paintings");
}
