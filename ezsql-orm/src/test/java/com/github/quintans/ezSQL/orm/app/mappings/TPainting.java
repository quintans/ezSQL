package com.github.quintans.ezSQL.orm.app.mappings;

import static com.github.quintans.ezSQL.dml.Definition.*;

import com.github.quintans.ezSQL.db.*;

public class TPainting {
    public static final Table T_PAINTING = new Table("PAINTING");

    public static final Column<Long> C_ID = T_PAINTING
    		.BIGINT("ID").key(Long.class);
    public static final Column<Integer> C_VERSION = T_PAINTING
    		.INTEGER("VERSION").version();
    public static final Column<String> C_NAME = T_PAINTING
    		.VARCHAR("NAME");
    public static final Column<Double> C_PRICE = T_PAINTING
    		.DECIMAL("PRICE");
    // FKs columns
    public static final Column<Long> C_ARTIST = T_PAINTING
    		.BIGINT("ARTIST_ID");
    public static final Column<Long> C_IMAGE = T_PAINTING
    		.BIGINT("IMAGE_ID").AS("imageFk");

    // MANY Paintings have ONE Artist
    public static final Association A_ARTIST = T_PAINTING
            .ASSOCIATE(C_ARTIST).TO(TArtist.C_ID).AS("artist");

    // ONE Painting has ONE Artist
    public static final Association A_IMAGE = T_PAINTING
            .ASSOCIATE(C_IMAGE).TO(TImage.C_ID).AS("image");

    // many to many
    // ONE painting has MANY galleries
    public static final Association A_GALLERIES = new Association(
            "galleries",
            ASSOCIATE(C_ID).TO(TGallery.GalleryPainting.C_PAINTING),
            ASSOCIATE(TGallery.GalleryPainting.C_GALLERY).TO(TGallery.C_ID)
            );
}
