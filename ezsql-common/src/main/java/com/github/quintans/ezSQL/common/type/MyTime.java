package com.github.quintans.ezSQL.common.type;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

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

    public MyTime(long time) {
        super(time);
        truncate();
    }

    private void truncate() {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        cal.setTime(this);
        cal.set(Calendar.YEAR, 1970);
        cal.set(Calendar.MONTH, 1);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.MILLISECOND, 0);
        setTime(cal.getTimeInMillis());
    }

    @Override
    public String toString() {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        cal.setTime(this);

        StringBuilder sb = new StringBuilder(12);
        sb.append(pad(cal.get(Calendar.HOUR_OF_DAY), 2))
                .append(":")
                .append(pad(cal.get(Calendar.MINUTE), 2))
                .append(":")
                .append(pad(cal.get(Calendar.SECOND), 2));

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

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof MyTime)) {
            return false;
        }

        MyTime other = (MyTime) obj;
        return getTime() == other.getTime();
    }

}
