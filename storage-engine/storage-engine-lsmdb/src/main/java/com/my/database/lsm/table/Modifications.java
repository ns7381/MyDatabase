package com.my.database.lsm.table;

import java.util.*;

public class Modifications extends TreeMap<String, Modification> {
    private int bytesNum;
    private boolean immutable;
    private int bytesLimit = Config.memTableBytesLimit;

    Modifications() {
        bytesNum = 0;
        immutable = false;
    }

    public Modifications(int bytesLimit) {
        this.bytesLimit = bytesLimit;
        bytesNum = 0;
        immutable = false;
    }

    @Override
    public Modification put(String key, Modification curr) {
        if (immutable) {
            throw new UnsupportedOperationException();
        }
        // count modifications bytes before put memory tree set
        if (containsKey(key)) {
            Modification last = get(key);
            if (last.getTimestamp() > curr.getTimestamp()) {
                return null;
            }
            if (last.isPut()) {
                bytesNum -= byteLen(last.getVal());
            }
            if (curr.isPut()) {
                bytesNum += byteLen(curr.getVal());
            }
        } else {
            if (curr.isPut()) {
                bytesNum += byteLen(curr.getVal());
            }
            bytesNum += Long.BYTES;
            bytesNum += byteLen(key);
        }
        return super.put(key, curr);
    }
    public void putAll(Modifications m) {
        for (Map.Entry<String, Modification> entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }
    public Modifications poll() {
        if (!exceedLimit()) {
            Modifications ret = new Modifications();
            clear();
            return ret;
        } else {
            Modifications ret = new Modifications(bytesLimit);
            List<String> shouldClean = new ArrayList<>();
            for (String r : keySet()) {
                ret.put(r, get(r));
                shouldClean.add(r);
                if (ret.exceedLimit()) {
                    break;
                }
            }
            for (String sc : shouldClean) {
                remove(sc);
            }
            return ret;
        }
    }
    public TreeSet<String> getSortedKeySet() {
        return new TreeSet<String>(keySet());
    }
    public String firstRow() {
        TreeSet<String> s = new TreeSet<>(super.keySet());
        return s.first();
    }

    public String lastRow() {
        TreeSet<String> s = new TreeSet<>(super.keySet());
        return s.last();
    }
    private static int byteLen(String s) {
        return s.getBytes().length;
    }

    public boolean exceedLimit() {
        return bytesNum >= bytesLimit;
    }
}
