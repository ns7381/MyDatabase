package com.my.database.lsm.table;


import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

@Getter
@Setter
public class Col implements Comparable<Col> {
    private String val;
    private long timestamp;

    private Col(String val) {
        this(val, System.currentTimeMillis());
    }

    Col(String val, long timestamp) {
        this.val = val;
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Col) {
            final Col that = (Col) o;
            return Objects.equals(this.val, that.val) && this.timestamp == that.timestamp;
        }
        return false;
    }

    @Override
    public int compareTo(Col other) {
        return (int) (this.timestamp - other.getTimestamp());
    }
}
