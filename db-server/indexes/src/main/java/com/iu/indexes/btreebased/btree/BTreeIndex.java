package com.iu.indexes.btreebased.btree;

import com.iu.indexes.Index;
import com.iu.indexes.util.FileUtil;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * the properties of b tree
 * For a tree to be classified as a B-tree, it must fulfill the following conditions:
 *
 * - the nodes in a B-tree of order m can have a maximum of m children
 * - each internal node (non-leaf and non-root) can have at least (m/2) children (rounded up)
 * - the root should have at least two children – unless it’s a leaf
 * - a non-leaf node with k children should have k-1 keys
 * - all leaves must appear on the same level
 */
public class BTreeIndex implements Index {
    private static final Logger LOGGER = Logger.getLogger(BTreeIndex.class.getName());
    private static final int ORDER = 5; // Порядок B-дерева
//    private static final String FILE_NAME = "btree.dat"; // Имя файла для сохранения данных
    private String fileName;
    private Node root;

    // Конструктор B-дерева
    public BTreeIndex(String fileName) {
        this.fileName = fileName;
        root = new Node(fileName); // Создаем корневой узел
    }

    // Метод вставки значения в B-дерево
    public void insert(int key, Object value) {
        root.insert(key, value); // Вызываем метод вставки у корневого узла
        saveToFile(); // Сохраняем изменения в файл
    }

    // Метод поиска значения по ключу в B-дереве
    public Object search(int key) {
        return root.search(key); // Вызываем метод поиска у корневого узла
    }

    // Класс узла B-дерева
    private static class Node implements Serializable {
        private final List<Integer> keys;
        private final List<Object> values;
        private final List<Long> childrenOffsets; // Смещения дочерних узлов в файле
        private boolean isLeaf;
        private String fileName;

        // Конструктор узла
        Node(String fileName) {
            keys = new ArrayList<>();
            values = new ArrayList<>();
            childrenOffsets = new ArrayList<>();
            isLeaf = true;
            this.fileName = fileName;
        }

        // Метод вставки значения в узел
        void insert(int key, Object value) {
            int index = findIndex(key); // Находим индекс, куда вставить ключ
            keys.add(index, key); // Вставляем ключ
            values.add(index, value); // Вставляем значение

            if (keys.size() > ORDER - 1) {
                split(); // Если узел переполнен, разделяем его
            }
        }

        // Метод поиска значения по ключу в узле
        Object search(int key) {
            int index = findIndex(key); // Находим индекс ключа
            if (index < keys.size() && keys.get(index) == key) {
                return values.get(index); // Если ключ найден, возвращаем значение
            } else if (isLeaf) {
                return null; // Если узел листовой и ключ не найден, возвращаем null
            } else {
                return Objects.requireNonNull(getChild(index)).search(key); // Рекурсивный поиск в дочернем узле
            }
        }

        // Метод для разделения переполненного узла
        private void split() {
            int mid = keys.size() / 2;
            Node left = new Node(fileName);
            left.keys.addAll(keys.subList(0, mid));
            left.values.addAll(values.subList(0, mid));
            if (!isLeaf) {
                left.childrenOffsets.addAll(childrenOffsets.subList(0, mid));
            }
            Node right = new Node(fileName);
            right.keys.addAll(keys.subList(mid + 1, keys.size()));
            right.values.addAll(values.subList(mid + 1, values.size()));
            if (!isLeaf) {
                right.childrenOffsets.addAll(childrenOffsets.subList(mid + 1, childrenOffsets.size()));
            }
            Integer midKey = keys.get(mid);
            Object midVal = values.get(mid);

            keys.clear();
            values.clear();
            childrenOffsets.clear();
            keys.add(midKey);
            values.add(midVal);
            childrenOffsets.add(writeNode(left)); // Записываем левый узел в файл и сохраняем смещение
            childrenOffsets.add(writeNode(right)); // Записываем правый узел в файл и сохраняем смещен

            isLeaf = false;
        }

        // Метод для нахождения индекса ключа
        private int findIndex(int key) {
            int index = 0;
            while (index < keys.size() && key > keys.get(index)) {
                index++;
            }
            return index;
        }

        // Метод для получения дочернего узла по индексу
        private Node getChild(int index) {
            return (Node) FileUtil.getChildNodeFromFile(fileName, childrenOffsets.get(index));
//            try (RandomAccessFile file = new RandomAccessFile(FILE_NAME, "r")) {
//                file.seek(childrenOffsets.get(index));
//                ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(Paths.get(FILE_NAME)));
//                return (Node) ois.readObject(); // Считываем дочерний узел из файла по смещению
//            } catch (IOException | ClassNotFoundException e) {
//                e.printStackTrace();
//            }
//            return null;
        }
    }

    // Метод для сохранения узла в файл и получения смещения
    private static long writeNode(Node node) {
        return FileUtil.writeNodeToFile(node.fileName, node);
//        try (RandomAccessFile file = new RandomAccessFile(FILE_NAME, "rw")) {
//            long offset = file.length(); // Получаем текущее смещение в файле
//            file.seek(offset);
//            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(FILE_NAME, true));
//            oos.writeObject(node); // Записываем узел в файл
//            return offset;
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return -1;
    }

    // Метод для сохранения данных в файл
    private void saveToFile() {
        FileUtil.saveTreeToFile(fileName, root);
//        try (ObjectOutputStream oos = new ObjectOutputStream(Files.newOutputStream(Paths.get(FILE_NAME)))) {
//            oos.writeObject(root); // Записываем корневой узел в файл
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    // Метод для загрузки данных из файла
    private void loadFromFile() {
        root = (Node) FileUtil.loadTreeFromFile(fileName);
//        try (ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(Paths.get(FILE_NAME)))) {
//            root = (Node) ois.readObject(); // Считываем корневой узел из файла
//        } catch (IOException | ClassNotFoundException e) {
//            e.printStackTrace();
//        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BTreeIndex index = (BTreeIndex) o;
        return Objects.equals(fileName, index.fileName);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(fileName);
    }
}
