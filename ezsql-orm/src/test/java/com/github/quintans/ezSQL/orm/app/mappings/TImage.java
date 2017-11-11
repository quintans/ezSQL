package com.github.quintans.ezSQL.orm.app.mappings;

import com.github.quintans.ezSQL.db.Association;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.toolkit.io.BinStore;

public class TImage {
    public static final Table T_IMAGE = new Table("IMAGE");

    public static final Column<Long> C_ID = T_IMAGE
    		.BIGINT("ID").key();
    public static final Column<Integer> C_VERSION = T_IMAGE
    		.INTEGER("VERSION").version();
    public static final Column<BinStore> C_CONTENT = T_IMAGE
    		.BIN("CONTENT");

    // ONE Image has ONE Painting
    public static final Association A_PAINTING = T_IMAGE
            .ASSOCIATE(C_ID)
            .TO(TPainting.C_IMAGE)
            .AS("painting");
}
