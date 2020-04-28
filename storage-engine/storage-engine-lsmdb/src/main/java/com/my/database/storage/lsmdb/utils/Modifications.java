package com.my.database.storage.lsmdb.utils;


import com.my.database.storage.lsmdb.io.interfaces.Filter;
import com.my.database.storage.lsmdb.io.interfaces.WritableFilter;

import java.util.*;

public class Modifications extends TreeMap<String, Modification> {
    private final int bytesLimit;
    private int bytesNum;
    private boolean immutable;

    public Modifications(int limit) {
        bytesLimit = limit;
        bytesNum = 0;
        immutable = false;
    }

    public Modifications(Modifications other) {
        this.bytesLimit = other.bytesLimit;
        this.bytesNum = other.bytesNum;
        immutable = false;
        putAll(other);
    }

    private static int byteLen(String s) {
        return s.getBytes().length;
    }

    static Modifications merge(Modifications m1, Modifications m2) {
        Modifications m = new Modifications(Integer.MAX_VALUE);
        for (String r : m1.rows()) {
            m.put(r, m1.get(r));
        }
        for (String r : m2.rows()) {
            m.put(r, m2.get(r));
        }
        return m;
    }

    /**
     * Merge two Modifications together, and split them again with the first one full of entries
     *
     * @param m1    first Modifications
     * @param m2    second Modifications
     * @param limit the byte limit for each Modifications
     * @return array of Modifications
     */

    static Queue<Modifications> reassign(Modifications m1, Modifications m2, int limit) {
        Queue<Modifications> mods = new LinkedList<>();
        Modifications total = merge(m1, m2);
        Modifications m = new Modifications(limit);
        for (Map.Entry<String, Modification> entry : total.entrySet()) {
            if (m.existLimit()) {
                mods.offer(m);
                m = new Modifications(limit);
            }
            m.put(entry.getKey(), entry.getValue());
        }
        mods.offer(m);
        return mods;
    }

    /**
     * Gets the immutable reference of the modifications
     *
     * @param mods the mods to refer
     * @return an immutable reference of the modifications
     */
    public static Modifications immutableRef(Modifications mods) {
        Modifications ref = new Modifications(mods);
        ref.immutable = true;
        return ref;
    }

    @Override
    public Modification put(String row, Modification curr) {
        if (immutable) {
            throw new UnsupportedOperationException();
        }
        if (containsKey(row)) {
            Modification last = get(row);
            if (last.getTimestamp() > curr.getTimestamp()) {
                return null;
            }
            if (last.isPut()) {
                assert last.getIfPresent() != null;
                bytesNum -= byteLen(last.getIfPresent().get());
            }
            if (curr.isPut()) {
                assert curr.getIfPresent() != null;
                bytesNum += byteLen(curr.getIfPresent().get());
            }
        } else {
            if (curr.isPut()) {
                assert curr.getIfPresent() != null;
                bytesNum += byteLen(curr.getIfPresent().get());
            }
            bytesNum += Long.BYTES;
            bytesNum += byteLen(row);
        }
        super.put(row, curr);
        return curr;
    }

    public void putAll(Modifications m) {
        for (Map.Entry<String, Modification> entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    public boolean existLimit() {
        return bytesNum >= bytesLimit;
    }

    int bytesNum() {
        return bytesNum;
    }

    public Set<String> rows() {
        return super.keySet();
    }

    public String firstRow() {
        TreeSet<String> s = new TreeSet<>(super.keySet());
        return s.first();
    }

    public String lastRow() {
        TreeSet<String> s = new TreeSet<>(super.keySet());
        return s.last();
    }

    public Filter calculateFilter(WritableFilter f) {
        for (String row : rows()) {
            f.add(row);
        }
        return f;
    }

    public boolean offer(Modifications modifications) {
        putAll(modifications);
        return existLimit();
    }

    @Override
    public void clear() {
        bytesNum = 0;
        immutable = false;
        super.clear();
    }

    public Modifications poll() {
        if (!existLimit()) {
            Modifications ret = new Modifications(this);
            clear();
            return ret;
        } else {
            Modifications ret = new Modifications(bytesLimit);
            List<String> shouldClean = new ArrayList<>();
            for (String r : rows()) {
                ret.put(r, get(r));
                shouldClean.add(r);
                if (ret.existLimit()) {
                    break;
                }
            }
            for (String sc : shouldClean) {
                remove(sc);
            }
            return ret;
        }
    }
}
