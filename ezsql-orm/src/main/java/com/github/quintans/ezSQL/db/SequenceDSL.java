package com.github.quintans.ezSQL.db;

public abstract class SequenceDSL {
  private String name;

  public SequenceDSL(String name) {
    this.name = name;
  }

  public String getName() {
    return this.name;
  }

  public Long fetchSequenceNextValue() {
    return fetchSequence(true);
  }

  public Long fetchSequenceCurrentValue() {
    return fetchSequence(false);
  }

  public abstract Long fetchSequence(boolean nextValue);

}
