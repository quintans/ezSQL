package com.github.quintans.ezSQL.orm.app.domain;

import java.util.Date;

import com.github.quintans.ezSQL.common.type.MyDate;
import com.github.quintans.ezSQL.common.type.MyDateTime;
import com.github.quintans.ezSQL.common.type.MyTime;

public class Temporal extends IdentityDomain<Long> {
    private MyTime clock;
    private MyDate today;
    private MyDateTime now;
    private Date instant;

    public MyTime getClock() {
        return clock;
    }

    public void setClock(MyTime clock) {
        this.clock = clock;
    }

    public MyDate getToday() {
        return today;
    }

    public void setToday(MyDate today) {
        this.today = today;
    }

    public MyDateTime getNow() {
        return now;
    }

    public void setNow(MyDateTime now) {
        this.now = now;
    }

    public Date getInstant() {
        return instant;
    }

    public void setInstant(Date instant) {
        this.instant = instant;
    }

    @Override
    public String toString() {
        return "Temporal [clock=" + clock + ", today=" + today + ", now=" + now + ", instant=" + instant + "]";
    }

}
