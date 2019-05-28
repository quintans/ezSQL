package com.github.quintans.ezSQL.toolkit.utils;

public class Pair<L, R> {
  private final L left;
  private final R right;

  public static <X, Y> Pair<X, Y> of(X left, Y right) {
    return new Pair<>(left, right);
  }

  public Pair(L left, R right) {
    this.left = left;
    this.right = right;
  }

  public L getLeft() {
    return left;
  }

  public R getRight() {
    return right;
  }
}
