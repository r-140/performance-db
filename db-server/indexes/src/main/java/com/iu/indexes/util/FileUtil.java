package com.iu.indexes.util;

import com.files.FileHelper;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FileUtil {

    public static long writeNodeToFile(String fileName, Object node) {
        if(!FileHelper.isFileExist(fileName)) {
            throw new IllegalStateException(String.format("Index file name %s does not exist", fileName));
        }
        try (RandomAccessFile file = new RandomAccessFile(fileName, "rw")) {
            long offset = file.length(); // Получаем текущее смещение в файле
            file.seek(offset);
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(fileName, true));
            oos.writeObject(node); // Записываем узел в файл
            return offset;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public static void saveTreeToFile(String fileName, Object root) {
        try (ObjectOutputStream oos = new ObjectOutputStream(Files.newOutputStream(Paths.get(fileName)))) {
            oos.writeObject(root); // Записываем корневой узел в файл
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Object loadTreeFromFile(String fileName) {
        try (ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(Paths.get(fileName)))) {
            return ois.readObject(); // Считываем корневой узел из файла
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Object getChildNodeFromFile(String fileName, long offset) {
        try (RandomAccessFile file = new RandomAccessFile(fileName, "r")) {
            file.seek(offset);
            ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(Paths.get(fileName)));
            return  ois.readObject(); // Считываем дочерний узел из файла по смещению
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }
}
