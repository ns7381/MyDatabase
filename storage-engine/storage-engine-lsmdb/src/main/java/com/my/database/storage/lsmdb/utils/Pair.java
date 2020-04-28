package com.my.database.storage.lsmdb.utils;

import java.util.Comparator;
import java.util.Objects;

public class Pair<Left, Right> {
    public final Left left;
    public final Right right;

    public Pair(Left left, Right right) {
        this.left = left;
        this.right = right;
    }

    public static <Left extends Comparable<Left>, Right extends Comparable<Right>>
    Comparator<Pair<Left, Right>> comparator() {
        return (Pair<Left, Right> o1, Pair<Left, Right> o2) -> {
            if (o1.left.compareTo(o2.left) < 0) {
                return -1;
            }
            if (o1.left.compareTo(o2.left) > 0) {
                return 1;
            }
            return Integer.compare(o1.right.compareTo(o2.right), 0);
        };
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Pair)) {
            return false;
        }
        Pair that = (Pair) o;
        return Objects.equals(this.left, that.left) && Objects.equals(this.right, that.right);
    }

    @Override
    public String toString() {
        return "(" + left + ", " + right + ")";
    }
}
