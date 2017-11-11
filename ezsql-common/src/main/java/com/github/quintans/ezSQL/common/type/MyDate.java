package com.github.quintans.ezSQL.common.type;

import java.util.Calendar;
import java.util.Date;

/**
 * Local date. No time zone, and no time part
 * 
 * @author paulo.quintans
 *
 */
public class MyDate extends Date {
    private static final long serialVersionUID = 1L;

    public MyDate() {
        this(System.currentTimeMillis());
    }

    public MyDate(long date) {
        super(date);
    }

    @Override
    public String toString() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(this);

        StringBuilder sb = new StringBuilder(10);
        sb.append(cal.get(Calendar.YEAR))
                .append("-")
                .append(pad(cal.get(Calendar.MONTH) + 1))
                .append("-")
                .append(pad(cal.get(Calendar.DAY_OF_MONTH)));

        return sb.toString();
    }

    private String pad(int value) {
        return value < 10 ? "0" + value : String.valueOf(value);
    }
}
