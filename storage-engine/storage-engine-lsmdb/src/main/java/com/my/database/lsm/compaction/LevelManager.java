package com.my.database.lsm.compaction;


import com.my.database.lsm.block.BlockDumper;
import com.my.database.lsm.block.BlockLoader;
import com.my.database.lsm.block.DataBlock;
import com.my.database.lsm.block.TempDataBlock;
import com.my.database.lsm.table.*;
import com.my.database.lsm.filter.BloomFilter;
import com.my.database.lsm.filter.WritableFilter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LevelManager {
    private final Descriptor descriptor;
    private final int level;
    private final int levelBlocksLimit;
    private final String column;
    private ReentrantReadWriteLock lock;
    private volatile boolean shouldWait;
    private final static String BLOCK_FILENAME_SUFFIX = ".db";
    private final static String TEMP_BLOCK_FILENAME_SUFFIX = ".db.tmp";


    // TODO: parallelize compactions in the future
    public LevelManager(Descriptor desc, String column, int level) {
        this.descriptor = desc;
        this.level = level;
        levelBlocksLimit = Config.blocksNumLimitForLevel.apply(level);
        this.column = column;
        this.lock = new ReentrantReadWriteLock(true);
        this.shouldWait = false;
    }

    public synchronized void lock() {
        shouldWait = true;
        lock.writeLock().lock();
    }

    public synchronized void unlock() {
        shouldWait = false;
        notifyAll();
        lock.writeLock().unlock();
    }

    public Modifications compact(Modifications modifications) throws IOException {
        // the first block get compacted in the level
        if (!getIndexBlockFile().exists()) {
            dumpBlock(modifications);
        } else {
            compactWithExist(modifications);
        }
        return collect();
    }

    public String get(String row) throws InterruptedException, NoSuchElementException, IOException {
        synchronized (this) {
            while (shouldWait) {
                this.wait();
            }
        }
        int index = BlockLoader.lookupIndex(getIndexBlockFile(), row);
        if (index != -1) {
            Modification mod = BlockLoader.loadModification(getDataBlockFile(index), row);
            if (mod.isPut()) {
                return mod.getVal();
            } else {
                return null;
            }
        }
        return null;
    }

    private Modifications collect() throws IOException {
        Modifications forNext = null;

        //requires the compact() method to remove original data files that are compacted
        ArrayList<File> files = mergeTempAndDataBlocks();
        // must from higher index to lower
        for (int i = files.size() - 1; i >= 0; i--) {
            relinkAsDataBlock(files.get(i), i);
        }
        if (files.size() > levelBlocksLimit) {
            List<Pair<String, String>> indexes = BlockLoader.loadIndex(getIndexBlockFile());
            BlockDumper.dump(indexes.subList(0, levelBlocksLimit), getIndexBlockFile());
            forNext = new Modifications(Config.blockBytesLimit);
            for (int start = levelBlocksLimit; start < files.size(); start++) {
                DataBlock b = new DataBlock(descriptor, column, level, start);
                forNext.putAll(BlockLoader.loadModifications(b.getFile()));
                Files.delete(b.getFile().toPath());
            }
        }
        //TODO: return blocks to next level
        return forNext;
    }

    private ArrayList<File> mergeTempAndDataBlocks() throws IOException {
        DataBlock[] dbs = getDataBlocks();
        TempDataBlock[] tbs = getTempDataBlocks();
        int di = 0, ti = 0;
        ArrayList<File> files = new ArrayList<>();
        while (di < dbs.length && ti < tbs.length) {
            if (dbs[di].getIndex() < tbs[ti].getOriginIndex()) {
                files.add(dbs[di++].getFile());
            } else {
                files.add(tbs[ti++].getFile());
            }
        }
        while (di < dbs.length) {
            files.add(dbs[di++].getFile());
        }
        while (ti < tbs.length) {
            files.add(tbs[ti++].getFile());
        }
        System.out.println("to merge:");
        for (File f : files) {
            System.out.println(f.getName());
        }
        System.out.println("--------------");
        return files;
    }

    private void relinkAsDataBlock(File file, int index) throws IOException {
        File dst = new DataBlock(descriptor, column, level, index).getFile();
        if (file.getName().equals(dst.getName())) {
            return;
        }
        System.out.printf("link %s to %s\n", dst.getName(), file.getName());
        Files.createLink(dst.toPath(), file.toPath());
        Files.delete(file.toPath());
    }

    private void dumpBlock(Modifications modifications) throws IOException {
        WritableFilter filter = new BloomFilter();
        Modifications blockMod = new Modifications(Config.blockBytesLimit);

        List<Pair<String, String>> indexes;
        indexes = new ArrayList<>();
        Queue<Integer> q;

        int i = 0;
        for (String key : modifications.keySet()) {
            if (blockMod.exceedLimit()) {
                TreeSet<String> sortedKeySet = blockMod.getSortedKeySet();
                indexes.add(new Pair<>(sortedKeySet.first(), sortedKeySet.last()));
                BlockDumper.dump(blockMod, filter, getTmpDataBlockFile(i));
                blockMod = new Modifications(Config.blockBytesLimit);
                i++;
            }
            blockMod.put(key, modifications.get(key));
            filter.add(key);
        }
        // left over
        TreeSet<String> sortedKeySet = blockMod.getSortedKeySet();
        indexes.add(new Pair<>(sortedKeySet.first(), sortedKeySet.last()));
        BlockDumper.dump(blockMod, filter, getTmpDataBlockFile(i));
        BlockDumper.dump(indexes, getIndexBlockFile());
    }

    private void compactWithExist(Modifications modifications) throws IOException {
        List<Pair<String, String>> indexes = BlockLoader.loadIndex(getIndexBlockFile());
        TreeSet<String> sortedKeySet = modifications.getSortedKeySet();
        int fst = locateBlock(sortedKeySet.first(), indexes);
        int snd = locateBlock(sortedKeySet.last(), indexes);
        DataBlock[] dataBlocks = this.getDataBlocks();
        List<Pair<String, String>> newIndexes = new ArrayList<>(indexes.subList(0, fst));

        int j = 0;
        for (int i = fst; i <= snd; ++i) {
            Modifications diskMod = BlockLoader.loadModifications(dataBlocks[i].getFile());
            modifications.putAll(diskMod);
            if (modifications.exceedLimit()) {
                Modifications poll = modifications.poll();
                newIndexes.add(new Pair<>(poll.firstRow(), poll.lastRow()));
                BloomFilter bloomFilter = new BloomFilter();
                poll.keySet().forEach(bloomFilter::add);
                BlockDumper.dump(poll, bloomFilter, getTmpDataBlockFile(j++, fst));
            }
            Files.delete(dataBlocks[i].getFile().toPath());
        }

        while (!modifications.isEmpty()) {
            Modifications poll = modifications.poll();
            newIndexes.add(new Pair<>(poll.firstRow(), poll.lastRow()));
            BloomFilter bloomFilter = new BloomFilter();
            poll.keySet().forEach(bloomFilter::add);
            BlockDumper.dump(poll, bloomFilter, getTmpDataBlockFile(j++, fst));
        }

        newIndexes.addAll(indexes.subList(snd + 1, indexes.size()));
        BlockDumper.dump(newIndexes, getIndexBlockFile());
    }

    private TempDataBlock[] getTempDataBlocks() {
        String[] filenames = getColDir().list((dir, name) -> TempDataBlock.isTempDataBlock(name));
        if (filenames == null) {
            System.err.println("filenames array is null");
            System.exit(-1);
        }
        TempDataBlock[] tmpBlocks = new TempDataBlock[filenames.length];
        for (int i = 0; i < filenames.length; i++) {
            tmpBlocks[i] = TempDataBlock.fromFileName(descriptor, column, filenames[i]);
        }
        Arrays.sort(tmpBlocks);
        return tmpBlocks;
    }

    private DataBlock[] getDataBlocks() {
        String[] filenames = getColDir().list((dir, name) -> DataBlock.isDataBlockForLevel(name, level));
        assert filenames != null;
        DataBlock[] blocks = new DataBlock[filenames.length];
        for (int i = 0; i < filenames.length; i++) {
            blocks[i] = DataBlock.fromFileName(descriptor, column, filenames[i]);
        }
        Arrays.sort(blocks);
        return blocks;
    }

    private int locateBlock(String row, List<Pair<String, String>> indexes) {
        int i = indexes.size() - 1;
        while (i > 0 && indexes.get(i).left.compareTo(row) > 0) {
            --i;
        }
        return i;
    }

    private File getColDir() {
        File dir = new File(new File(Config.STORAGE_DIR + descriptor.getTable(), descriptor.getNs()), descriptor.getCf());
        File colDir = new File(dir, column);
        if (!colDir.exists()) {
            colDir.mkdirs();
        }
        return colDir;
    }

    private File getIndexBlockFile() {
        String idxFileName = String.format("%d_Index%s", level, BLOCK_FILENAME_SUFFIX);
        return new File(getColDir(), idxFileName);
    }

    private File getDataBlockFile(int index) {
        String filename = String.format("%d_%d_Data%s", level, index, BLOCK_FILENAME_SUFFIX);
        return new File(getColDir(), filename);
    }

    private File getTmpDataBlockFile(int index) {
        return getTmpDataBlockFile(index, 0);
    }

    private File getTmpDataBlockFile(int index, int originIndex) {
        String filename = String.format("%d_%d_Data%s_%d", level, originIndex, TEMP_BLOCK_FILENAME_SUFFIX, index);
        return new File(getColDir(), filename);
    }
}
