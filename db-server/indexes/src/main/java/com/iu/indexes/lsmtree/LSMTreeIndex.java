package com.iu.indexes.lsmtree;

import com.iu.indexes.Index;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class LSMTreeIndex implements Index {
    private final TreeMap<Integer, Object> memTable;
    private final String fileName;
    private final int mergeThreshold;
    private int levelCounter;

    public LSMTreeIndex(String fileName, int mergeThreshold) {
        this.fileName = fileName;
        this.memTable = new TreeMap<>();
        this.mergeThreshold = mergeThreshold;
        this.levelCounter = 0;
        initializeDisk();
    }

    private void initializeDisk() {
        File directory = new File(fileName);
        if (!directory.exists()) {
            directory.mkdirs();
        }
    }

    public void insert(int key, Object value) {
        memTable.put(key, value);
        if (memTable.size() >= mergeThreshold) {
            merge();
        }
    }

    public Object search(int key) {
        Object value = memTable.get(key);
        if (value != null) {
            return value;
        }
        for (int i = 0; i < levelCounter; i++) {
            TreeMap<Integer, Object> level = readLevelFromDisk(i);
            if (level.containsKey(key)) {
                return level.get(key);
            }
        }
        return null;
    }

    private void merge() {
        TreeMap<Integer, Object> level = new TreeMap<>(memTable);
        writeLevelToDisk(level, levelCounter++);
        memTable.clear();
    }

    private void writeLevelToDisk(TreeMap<Integer, Object> level, int levelNumber) {
        try (ObjectOutputStream oos = new ObjectOutputStream(
                Files.newOutputStream(Paths.get(fileName + "/level_" + levelNumber + ".dat")))) {
            oos.writeObject(level);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private TreeMap<Integer, Object> readLevelFromDisk(int levelNumber) {
        try (ObjectInputStream ois = new ObjectInputStream(
                Files.newInputStream(Paths.get(fileName + "/level_" + levelNumber + ".dat")))) {
            return (TreeMap<Integer, Object>) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return new TreeMap<>();
    }

    public void printDiskState() {
        for (int i = 0; i < levelCounter; i++) {
            TreeMap<Integer, Object> level = readLevelFromDisk(i);
            System.out.println("Level " + i + ": " + level);
        }
    }
}
