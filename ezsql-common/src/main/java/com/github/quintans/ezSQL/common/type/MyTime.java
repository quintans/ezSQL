package com.github.quintans.ezSQL.common.type;

import java.util.Calendar;
import java.util.Date;

/**
 * Local Time. No time zone and no date part.
 * 
 * @author paulo.quintans
 *
 */
public class MyTime extends Date {
    private static final long serialVersionUID = 1L;

    public MyTime() {
        this(System.currentTimeMillis());
    }

    public MyTime(long date) {
        super(date);
    }

    @Override
    public String toString() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(this);

        StringBuilder sb = new StringBuilder(12);
        sb.append(pad(cal.get(Calendar.HOUR_OF_DAY), 2))
                .append(":")
                .append(pad(cal.get(Calendar.MINUTE), 2))
                .append(":")
                .append(pad(cal.get(Calendar.SECOND), 2))
                .append(".")
                .append(pad(cal.get(Calendar.MILLISECOND), 3));

        return sb.toString();
    }

    private String pad(int value, int len) {
        StringBuilder sb = new StringBuilder(3);
        sb.append(value);
        for (int i = sb.length(); i < len; i++) {
            sb.append("0");
        }

        return sb.toString();
    }

}
