package com.github.quintans.ezSQL.orm.app.domain;

import com.github.quintans.ezSQL.transformers.Converter;

import java.awt.*;

public class ColorConverter implements Converter<Color, String> {
    private static final String SEPARATOR = "|";
    private static final String SEPARATOR_SPLIT = "\\|";

    /**
     * Convert Color object to a String
     * with format red|green|blue|alpha
     */
    @Override
    public String toDb(Color color) {
        if(color == null) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        sb.append(color.getRed()).append(SEPARATOR)
                .append(color.getGreen())
                .append(SEPARATOR)
                .append(color.getBlue())
                .append(SEPARATOR)
                .append(color.getAlpha());
        return sb.toString();
    }

    /**
     * Convert a String with format red|green|blue|alpha
     * to a Color object
     */
    @Override
    public Color fromDb(String colorString) {
        if(colorString == null) {
            return null;
        }

        String[] rgb = colorString.split(SEPARATOR_SPLIT);
        return new Color(Integer.parseInt(rgb[0]),
                Integer.parseInt(rgb[1]),
                Integer.parseInt(rgb[2]),
                Integer.parseInt(rgb[3]));
    }
}
