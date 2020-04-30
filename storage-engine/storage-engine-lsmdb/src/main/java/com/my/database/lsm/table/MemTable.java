package com.my.database.lsm.table;

import com.my.database.lsm.compaction.LevelManager;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.util.NoSuchElementException;

@Setter
@Getter
class MemTable {

    private boolean isClosed = false;
    private final String columnName;
    private final Descriptor descriptor;
    private final LevelManager[] levelManagers;
    private Modifications modifications = new Modifications();

    MemTable(String name, String columnName, String[] columnNames) {
        this.columnName = columnName;
        this.descriptor = new Descriptor(name, "", "", columnNames);
        levelManagers = new LevelManager[Config.onDiskLevelsLimit];
        for (int i = 1; i < Config.onDiskLevelsLimit; i++) {
            levelManagers[i] = new LevelManager(descriptor, columnName, i);
        }
    }

    void put(String key, String val) throws IOException {
        Modification modification = new Modification(val);

        modifications.put(key, modification);
        if (modifications.exceedLimit()) {
            Modifications modifications = new Modifications();
            modifications.putAll(cleanup());
            compact(modifications);
        }
    }

    public String get(String row) throws InterruptedException {
        if (modifications.containsKey(row)) {
            Modification modification = modifications.get(row);
            if (!modification.isPut()) {
                // if no put happened or the put is stale
                return null;
            } else {
                return modification.getVal();
            }
        }

        for (int i = 1; i < levelManagers.length; i++) {
            System.out.println("lookup level: " + i);
            try {
                return levelManagers[i].get(row);
            } catch (NoSuchElementException | IOException ignored) {
            }
        }

        return null;
    }

    private void compact(Modifications modifications) throws IOException {
        for (int i = 1; i < levelManagers.length; i++) {
            if (modifications == null || modifications.size() < 1) {
                break;
            }
            LevelManager levelManager = levelManagers[i];
            System.out.printf("compact begin for level %d\n", i);
            levelManager.lock();
            levelManager.compact(modifications);
            levelManager.unlock();
            System.out.printf("compact success for level %d\n\n", i);
        }
    }

    private synchronized Modifications cleanup() {
        Modifications origin = modifications;
        modifications = new Modifications();
        return origin;
    }
}
