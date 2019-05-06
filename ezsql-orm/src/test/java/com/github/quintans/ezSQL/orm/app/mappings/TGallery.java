package com.github.quintans.ezSQL.orm.app.mappings;

import static com.github.quintans.ezSQL.dml.Definition.*;

import com.github.quintans.ezSQL.db.*;

public class TGallery {
    public static final Table T_GALLERY = new Table("GALLERY");

    public static final Column<Long> C_ID = T_GALLERY
    		.BIGINT("ID").key(Long.class);
    public static final Column<Integer> C_VERSION = T_GALLERY
    		.INTEGER("VERSION").version();
    public static final Column<String> C_NAME = T_GALLERY
    		.VARCHAR("NAME");
    public static final Column<String> C_ADRESS = T_GALLERY
    		.VARCHAR("ADDRESS");

    // intermediary table
    public static class GalleryPainting {
        public static final Table T_GALLERY_PAINTING = new Table("GALLERY_PAINTING");
        public static final Column<Long> C_PAINTING = T_GALLERY_PAINTING
        		.BIGINT("PAINTING").key(Long.class);
        public static final Column<Long> C_GALLERY = T_GALLERY_PAINTING
        		.BIGINT("GALLERY").key(Long.class);
    }

    // many to many
    // ONE gallery has MANY paintings
    public static final Association A_PAINTINGS = new Association(
            "paintings",
            ASSOCIATE(C_ID).TO(GalleryPainting.C_GALLERY),
            ASSOCIATE(GalleryPainting.C_PAINTING).TO(TPainting.C_ID)
            );
}
