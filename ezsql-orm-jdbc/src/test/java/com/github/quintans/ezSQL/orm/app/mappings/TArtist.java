package com.github.quintans.ezSQL.orm.app.mappings;

import com.github.quintans.ezSQL.db.Association;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.orm.app.domain.EGender;

import java.util.Date;

public class TArtist {
  public static final Table T_ARTIST = new Table("ARTIST");

  public static final Column<Long> C_ID = T_ARTIST
      .BIGINT("ID").key(Long.class);
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
    T_ARTIST.setPreInsertTrigger(ins -> {
      if (ins.get(C_VERSION) == null) {
        ins.set(C_VERSION, 1);
      }
      ins.set(C_CREATION, new Date());
    });

    T_ARTIST.setPreUpdateTrigger(upd -> upd.set(C_MODIFICATION, new Date()));
  }

  // ONE artist has MANY paintings
  public final static Association A_PAINTINGS = T_ARTIST
      .ASSOCIATE(C_ID).TO(TPainting.C_ARTIST).AS("paintings");
}
