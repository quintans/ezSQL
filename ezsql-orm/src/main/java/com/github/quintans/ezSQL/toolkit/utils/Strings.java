package com.github.quintans.ezSQL.toolkit.utils;

public class Strings {
    public static String capitalizeFirst(String s){
        return s.substring(0, 1).toUpperCase() + s.substring(1);
    }

    public static String toCamelCase(String name) {
        if (name != null) {
            StringBuilder result = new StringBuilder(name.substring(0, 1).toLowerCase());
            int x = name.length();
            boolean toUpper = false;
            for (int i = 2; i <= x; i++) {
                String letter = name.substring(i - 1, i);
                if (letter.equals("_")) {
                    toUpper = true;
                } else {
                    if (toUpper) {
                        letter = letter.toUpperCase();
                        toUpper = false;
                    } else {
                        letter = letter.toLowerCase();
                    }
                    result.append(letter);
                }
            }
            return result.toString();
        }

        return null;
    }
}
