package com.iu.indexes.lsmtree;

import java.io.*;
import java.util.*;

class SSTable {
    private File file;

    public SSTable(File file) {
        this.file = file;
    }

    public void write(Map<Integer, Long> map) throws IOException {
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(file))) {
            for (Map.Entry<Integer, Long> entry : map.entrySet()) {
                out.writeInt(entry.getKey());
                out.writeUTF(entry.getValue() != null ? String.valueOf(entry.getValue()) : ""); // Запись значений с поддержкой удаления
            }
        }
    }

    public TreeMap<Integer, Long> read() throws IOException {
        TreeMap<Integer, Long> map = new TreeMap<>();
        try (DataInputStream in = new DataInputStream(new FileInputStream(file))) {
            while (in.available() > 0) {
                int key = in.readInt();
                String value = in.readUTF();
                if (!value.isEmpty()) {
                    map.put(key, Long.valueOf(value));
                }
            }
        }
        return map;
    }
}
