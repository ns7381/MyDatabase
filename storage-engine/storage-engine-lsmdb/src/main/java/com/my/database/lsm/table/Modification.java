package com.my.database.lsm.table;

import lombok.Getter;
import org.apache.commons.lang.StringUtils;

@Getter
public final class Modification {

    private final boolean isPut;
    private Col col;

    public Modification(long timestamp) {
        isPut = false;
        col = new Col(null, timestamp);
    }

    public Modification(String val) {
        if (StringUtils.isEmpty(val)) {
            isPut = false;
            col = new Col(null, System.currentTimeMillis());
        } else {
            isPut = true;
            col = new Col(val, System.currentTimeMillis());
        }
    }

    public Modification(String val, long timestamp) {
        isPut = true;
        col = new Col(val, timestamp);
    }

    public long getTimestamp() {
        return col.getTimestamp();
    }

    public String getVal() {
        return col.getVal();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Modification) {
            final Modification that = (Modification) o;
            return this.isPut == that.isPut &&
                    this.col.equals(that.col);
        }
        return true;
    }

}
