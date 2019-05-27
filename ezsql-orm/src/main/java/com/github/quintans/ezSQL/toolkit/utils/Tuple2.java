package com.github.quintans.ezSQL.toolkit.utils;

public class Tuple2<L, R> {
  private final L left;
  private final R right;

  public static <X, Y> Tuple2<X, Y> of(X left, Y right) {
    return new Tuple2<>(left, right);
  }

  public Tuple2(L left, R right) {
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
