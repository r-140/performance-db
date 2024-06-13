//package com.iu.indexes.btree;
//
//import com.iu.indexes.Index;
//
//import java.io.*;
//import java.nio.file.Files;
//import java.nio.file.Paths;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Objects;
//
//public class BTreeIndexOrig implements Index {
//    private static final int ORDER = 5; // Порядок B-дерева
//    private static final String FILE_NAME = "btree.dat"; // Имя файла для сохранения данных
//
//    private Node root;
//
//    // Конструктор B-дерева
//    public BTreeIndexOrig() {
//        root = new Node(); // Создаем корневой узел
//    }
//
//    // Метод вставки значения в B-дерево
//    public void insert(int key, Object value) {
//        root.insert(key, value); // Вызываем метод вставки у корневого узла
//        saveToFile(); // Сохраняем изменения в файл
//    }
//
//    // Метод поиска значения по ключу в B-дереве
//    public Object search(int key) {
//        return root.search(key); // Вызываем метод поиска у корневого узла
//    }
//
//    // Класс узла B-дерева
//    private static class Node implements Serializable {
//        private final List<Integer> keys;
//        private final List<Object> values;
//        private final List<Long> childrenOffsets; // Смещения дочерних узлов в файле
//        private boolean isLeaf;
//
//        // Конструктор узла
//        Node() {
//            keys = new ArrayList<>();
//            values = new ArrayList<>();
//            childrenOffsets = new ArrayList<>();
//            isLeaf = true;
//        }
//
//        // Метод вставки значения в узел
//        void insert(int key, Object value) {
//            int index = findIndex(key); // Находим индекс, куда вставить ключ
//            keys.add(index, key); // Вставляем ключ
//            values.add(index, value); // Вставляем значение
//
//            if (keys.size() > ORDER - 1) {
//                split(); // Если узел переполнен, разделяем его
//            }
//        }
//
//        // Метод поиска значения по ключу в узле
//        Object search(int key) {
//            int index = findIndex(key); // Находим индекс ключа
//            if (index < keys.size() && keys.get(index) == key) {
//                return values.get(index); // Если ключ найден, возвращаем значение
//            } else if (isLeaf) {
//                return null; // Если узел листовой и ключ не найден, возвращаем null
//            } else {
//                return Objects.requireNonNull(getChild(index)).search(key); // Рекурсивный поиск в дочернем узле
//            }
//        }
//
//        // Метод для разделения переполненного узла
//        private void split() {
//            int mid = keys.size() / 2;
//            Node left = new Node();
//            left.keys.addAll(keys.subList(0, mid));
//            left.values.addAll(values.subList(0, mid));
//            if (!isLeaf) {
//                left.childrenOffsets.addAll(childrenOffsets.subList(0, mid + 1));
//            }
//            Node right = new Node();
//            right.keys.addAll(keys.subList(mid + 1, keys.size()));
//            right.values.addAll(values.subList(mid + 1, values.size()));
//            if (!isLeaf) {
//                right.childrenOffsets.addAll(childrenOffsets.subList(mid + 1, childrenOffsets.size()));
//            }
//            keys.clear();
//            values.clear();
//            childrenOffsets.clear();
//            keys.add(keys.get(mid));
//            values.add(values.get(mid));
//            childrenOffsets.add(writeNode(left)); // Записываем левый узел в файл и сохраняем смещение
//            childrenOffsets.add(writeNode(right)); // Записываем правый узел в файл и сохраняем смещение
//            isLeaf = false;
//        }
//
//        // Метод для нахождения индекса ключа
//        private int findIndex(int key) {
//            int index = 0;
//            while (index < keys.size() && key > keys.get(index)) {
//                index++;
//            }
//            return index;
//        }
//
//        // Метод для получения дочернего узла по индексу
//        private Node getChild(int index) {
//            try (RandomAccessFile file = new RandomAccessFile(FILE_NAME, "r")) {
//                file.seek(childrenOffsets.get(index));
//                ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(Paths.get(FILE_NAME)));
//                return (Node) ois.readObject(); // Считываем дочерний узел из файла по смещению
//            } catch (IOException | ClassNotFoundException e) {
//                e.printStackTrace();
//            }
//            return null;
//        }
//    }
//
//    // Метод для сохранения узла в файл и получения смещения
//    private static long writeNode(Node node) {
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
//    }
//
//    // Метод для сохранения данных в файл
//    private void saveToFile() {
//        try (ObjectOutputStream oos = new ObjectOutputStream(Files.newOutputStream(Paths.get(FILE_NAME)))) {
//            oos.writeObject(root); // Записываем корневой узел в файл
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    // Метод для загрузки данных из файла
//    private void loadFromFile() {
//        try (ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(Paths.get(FILE_NAME)))) {
//            root = (Node) ois.readObject(); // Считываем корневой узел из файла
//        } catch (IOException | ClassNotFoundException e) {
//            e.printStackTrace();
//        }
//    }
//
//}
