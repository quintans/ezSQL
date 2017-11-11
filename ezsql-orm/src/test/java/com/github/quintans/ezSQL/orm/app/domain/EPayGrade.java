package com.github.quintans.ezSQL.orm.app.domain;

import com.github.quintans.ezSQL.common.api.Value;

public enum EPayGrade implements Value<Integer> {
    BOTTOM(1),
    LOW(2),
    MEDIUM(3),
    HIGH(4),
    TOP(5);

    private Integer value;

    private EPayGrade(Integer value) {
        this.value = value;
    }

    @Override
    public Integer value() {
        return this.value;
    }
}
