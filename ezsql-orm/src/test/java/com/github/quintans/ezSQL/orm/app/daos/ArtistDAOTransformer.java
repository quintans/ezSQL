package com.github.quintans.ezSQL.orm.app.daos;

import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.orm.app.domain.Artist;
import com.github.quintans.ezSQL.orm.app.domain.Painting;
import com.github.quintans.ezSQL.orm.app.mappings.TArtist;
import com.github.quintans.ezSQL.transformers.MapColumn;
import com.github.quintans.ezSQL.transformers.QueryMapper;
import com.github.quintans.ezSQL.transformers.Record;
import com.github.quintans.jdbc.exceptions.PersistenceException;

import java.util.LinkedHashSet;
import java.util.List;

public class ArtistDAOTransformer implements QueryMapper<Artist> {
    private Driver driver;
    private QueryMapper paintingMapper;

    public ArtistDAOTransformer(Driver driver) {
        this.driver = driver;
    }


    private QueryMapper getPaintingMapper() {
        if (paintingMapper == null) {
            paintingMapper = new PaintingDAOTransformer(driver);
        }
        return paintingMapper;
    }

    @Override
    public Object createFrom(Object parentInstance, String name) {
        if (parentInstance instanceof Artist) {
            if (TArtist.A_PAINTINGS.getAlias().equals(name)) {
                return getPaintingMapper().createFrom(parentInstance, name);
            }
        }

        return new Artist();
    }

    @Override
    public void link(Object instance, String name, Object value) {
        if (instance instanceof Artist) {
            Artist entity = (Artist) instance;

            if (TArtist.A_PAINTINGS.getAlias().equals(name)) {
                if (entity.getPaintings() == null) {
                    entity.setPaintings(new LinkedHashSet<>());
                }
                entity.getPaintings().add((Painting) value);
            }
        } else if (instance instanceof Painting) {
            getPaintingMapper().link(instance, name, value);
        }
    }

    @Override
    public boolean map(Record record, Object instance, List<MapColumn> mapColumns) {
        try {
            boolean touched = false;

            if (instance instanceof Artist) {
                Artist entity = (Artist) instance;

                for (MapColumn mapColumn : mapColumns) {
                    int idx = mapColumn.getIndex();
                    String alias = mapColumn.getAlias();

                    if (TArtist.C_ID.getAlias().equals(alias)) {
                        Long value = record.get(idx, Long.class);
                        entity.setId(value);
                        touched |= value != null;
                    } else if (TArtist.C_VERSION.getAlias().equals(alias)) {
                        Integer value = record.get(idx, Integer.class);
                        entity.setVersion(value);
                        touched |= value != null;
                    } else if (TArtist.C_NAME.getAlias().equals(alias)) {
                        String value = record.get(idx, String.class);
                        //entity.setName(value);
                        touched |= value != null;
                    }
                }
            } else if (instance instanceof Painting) {
                touched = getPaintingMapper().map(record, instance, mapColumns);
            }
            return touched;

        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }
}
