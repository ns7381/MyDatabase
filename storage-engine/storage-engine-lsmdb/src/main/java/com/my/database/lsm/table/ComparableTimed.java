package com.my.database.lsm.table;


import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

@Getter
@Setter
public class ComparableTimed<T> implements Comparable<ComparableTimed<T>> {
    private T row;
    private long timestamp;

    ComparableTimed(T row) {
        this(row, System.currentTimeMillis());
    }

    ComparableTimed(T row, long timestamp) {
        this.row = row;
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof ComparableTimed) {
            final ComparableTimed<T> that = (ComparableTimed<T>) o;
            return Objects.equals(this.row, that.row) && this.timestamp == that.timestamp;
        }
        return false;
    }

    @Override
    public int compareTo(ComparableTimed<T> other) {
        return (int) (this.timestamp - other.getTimestamp());
    }
}
