package com.my.database.storage.lsmdb.io.sstable;

import com.my.database.storage.lsmdb.io.sstable.blocks.Descriptor;
import com.my.database.storage.lsmdb.io.sstable.compaction.LevelManager;
import com.my.database.storage.lsmdb.utils.Modifications;
import com.my.database.storage.lsmdb.utils.Qualifier;
import com.my.database.storage.lsmdb.utils.Timed;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.*;

public class SSTable implements Flushable, Closeable {
    private final Descriptor desc;
    private final String column;
    private final LevelManager[] levelManagers;
    private final LinkedList<MemTable> memTables;
    private final int memTablesLimit;
    private SSTableConfig config;
    private boolean isClosed = false;

    public SSTable(Descriptor desc, String column, SSTableConfig config) {
        this.desc = desc;
        this.column = column;
        this.config = config;
        levelManagers = new LevelManager[config.getOnDiskLevelsLimit()];
        for (int i = 1; i < config.getOnDiskLevelsLimit(); i++) {
            levelManagers[i] = new LevelManager(desc, column, i, config);
        }
        memTables = new LinkedList<>();
        memTablesLimit = config.getMemTablesLimit();
        memTables.add(new MemTable(desc, column, config));
    }

    private void checkNotClosed() {
        if (isClosed) {
            throw new RuntimeException("SSTable is already closed");
        }
    }

    public Optional<String> get(String row) throws InterruptedException {
        checkNotClosed();
        Iterator<MemTable> descItr = memTables.descendingIterator();
        while (descItr.hasNext()) {
            try {
                String v = descItr.next().get(row);
                return Optional.of(v);
            } catch (NoSuchElementException ignored) {
            }
        }

        for (int i = 1; i < levelManagers.length; i++) {
            System.out.println("lookup level: " + i);
            try {
                if (levelManagers[i].isEmpty()) {
                    return Optional.empty();
                }
                return levelManagers[i].get(row);
            } catch (NoSuchElementException ignored) {
            }
        }

        return Optional.empty();
    }

    /**
     * Go through all the MemTables and Levels of datablocks to find entries meet the qualifier
     */
    public Map<String, String> getColumnWithQualifier(Qualifier q) throws IOException, InterruptedException {
        Map<String, Timed<String>> result = new HashMap<>();
        for (int i = 1; i < config.getOnDiskLevelsLimit(); i++) {
            result = this.mergeEntryMaps(result, this.levelManagers[i].getColumnWithQualifier(q));
        }
        for (MemTable memTable : memTables) {
            result = this.mergeEntryMaps(result, memTable.getColumnWithQualifier(q));
        }

        Map<String, String> toReturn = new HashMap<>();
        for (Map.Entry<String, Timed<String>> entry : result.entrySet()) {
            String value = entry.getValue().get();
            if (value.length() > 0) {
                toReturn.put(entry.getKey(), value);
            }
        }
        return toReturn;
    }

    private Map<String, Timed<String>> mergeEntryMaps(
            Map<String, Timed<String>> m1,
            Map<String, Timed<String>> m2
    ) {
        Map<String, Timed<String>> mergedMap = new HashMap<>();
        mergeMap(m1, mergedMap);
        mergeMap(m2, mergedMap);
        return mergedMap;
    }

    private void mergeMap(Map<String, Timed<String>> m1, Map<String, Timed<String>> mergedMap) {
        for (Map.Entry<String, Timed<String>> entry : m1.entrySet()) {
            String rowKey = entry.getKey();
            Timed<String> timedValue = entry.getValue();
            if (!mergedMap.containsKey(rowKey)
                    || mergedMap.get(rowKey).getTimestamp() < timedValue.getTimestamp()) {
                mergedMap.put(entry.getKey(), entry.getValue());
            }
        }
    }

    public synchronized boolean put(String row, String val) throws IOException {
        checkNotClosed();
        if (row.length() == 0) {
            return false;
        }
        try {
            if (val != null && !val.isEmpty()) {
                memTables.getLast().put(row, val);
            } else {
                memTables.getLast().remove(row);
            }
        } catch (MemTable.MemTableFullException e) {
            if (memTables.size() < memTablesLimit) {
                return memTables.add(new MemTable(desc, column, config));
            }
            Modifications mods = config.getMemTablesFlushStrategy().apply(memTables);
            compactModifications(mods);
        }
        return true;
    }

    @Override
    public void flush() throws IOException {
        checkNotClosed();
        Modifications mods = new Modifications(config.getBlockBytesLimit());
        while (!memTables.isEmpty()) {
            mods.offer(memTables.removeFirst().stealModifications());
        }
        compactModifications(mods);
    }

    private void compactModifications(Modifications mods) throws IOException {
        for (int i = 1; i < levelManagers.length; i++) {
            if (mods == null) {
                break;
            }
            LevelManager levelManager = levelManagers[i];
            System.out.printf("compact begin for level %d\n", i);
            levelManager.freeze();
            mods = levelManager.compact(mods);
            levelManager.unfreeze();
            System.out.printf("compact success for level %d\n\n", i);
        }

        if (mods != null) {
            throw new RuntimeException("out of storage");
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        isClosed = true;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        close();
    }
}
