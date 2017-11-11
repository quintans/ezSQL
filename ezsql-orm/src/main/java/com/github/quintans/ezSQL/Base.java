package com.github.quintans.ezSQL;

import java.util.List;

import com.github.quintans.ezSQL.dml.Condition;
import com.github.quintans.ezSQL.dml.Definition;
import com.github.quintans.ezSQL.dml.Function;

public abstract class Base<T> {

    // CONDITION ===========================

    public Condition iMatches(T value) {
        return Definition.iMatches(this, value);
    }

    public Condition like(T value) {
        return Definition.like(this, value);
    }

    public Condition iLike(T value) {
        return Definition.iLike(this, value);
    }

    public Condition gt(T value) {
        return Definition.greater(this, value);
    }

    public Condition gtoe(T value) {
        return Definition.greaterOrMatch(this,value);
    }

    public Condition lt(T value) {
        return Definition.lesser(this, value);
    }

    public Condition ltoe(T value) {
        return Definition.lesserOrMatch(this, value);
    }

    public Condition is(T value) {
        if(value == null)
            return isNull();
        else
            return Definition.is(this, value);
    }

    public Condition different(T value) {
        if(value == null)
            return isNotNull();
        else
            return Definition.different(this, value);
    }

    public Condition isNull() {
        return Definition.isNull(this);
    }

    public Condition isNotNull() {
        return isNull().not();
    }

    public Condition not() {
        return Definition.not(this);
    }

    public Condition in(List<?> values) {
        return Definition.in(this, values);
    }

    public Condition in(T... value) {
        return Definition.in(this, value);
    }

    public Condition range(T left, T right) {
        return Definition.range(this, left, right);
    }

    // conditions with functions
    
    public Condition iMatches(Function value) {
        return Definition.iMatches(this, value);
    }

    public Condition like(Function value) {
        return Definition.like(this, value);
    }

    public Condition iLike(Function value) {
        return Definition.iLike(this, value);
    }

    public Condition gt(Function value) {
        return Definition.greater(this, value);
    }

    public Condition gtoe(Function value) {
        return Definition.greaterOrMatch(this,value);
    }

    public Condition lt(Function value) {
        return Definition.lesser(this, value);
    }

    public Condition ltoe(Function value) {
        return Definition.lesserOrMatch(this, value);
    }

    public Condition is(Function value) {
        if(value == null)
            return isNull();
        else
            return Definition.is(this, value);
    }

    public Condition different(Function value) {
        if(value == null)
            return isNotNull();
        else
            return Definition.different(this, value);
    }

    public Condition in(Function... value) {
        return Definition.in(this, value);
    }

    public Condition range(Function left, Function right) {
        return Definition.range(this, left, right);
    }

}
