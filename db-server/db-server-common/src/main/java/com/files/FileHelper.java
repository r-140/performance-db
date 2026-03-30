package com.files;

import com.util.CommonConsts;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class FileHelper {
    private static final Logger LOGGER = Logger.getLogger(FileHelper.class.getName());

    public static void removeLineFromFile(String filePath, String lineToRemove) throws IOException {
        File inputFile = new File(filePath);
        File tempFile  = new File(inputFile.getAbsolutePath() + ".tmp");

        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile));
             BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().equals(lineToRemove)) {
                    writer.write(line + System.lineSeparator());
                }
            }
        }

        if (!inputFile.delete()) {
            LOGGER.warning("Could not delete original file: " + filePath);
            return;
        }
        if (!tempFile.renameTo(inputFile)) {
            LOGGER.warning("Could not rename temp file to: " + filePath);
        }
    }

    public static long writeToFile(final String pathToFile, final String data, boolean isAppend)
            throws IOException {
        String sanitised = data.replaceAll(System.lineSeparator(), "") + System.lineSeparator();
        // try-with-resources — original leaked the FileOutputStream
        try (FileOutputStream fos = new FileOutputStream(pathToFile, isAppend)) {
            long offset = fos.getChannel().position();
            fos.write(sanitised.getBytes());
            return offset;
        }
    }

    public static Map<Integer, Long> readFile(String file, boolean isRecover) throws IOException {
        LOGGER.log(Level.FINEST, "readFile: " + file + " recover=" + isRecover);
        // Use "r" — no need for write access when reading
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            Map<Integer, Long> docMap = isRecover
                    ? new HashMap<>()
                    : new ConcurrentHashMap<>();
            long pos = 0;
            while (true) {
                raf.seek(pos);
                String line = raf.readLine();
                if (line == null || line.isEmpty()) break;

                String[] tok = isRecover
                        ? line.split(CommonConsts.HASH_SNAPSHOT_SEPARATOR, 2)
                        : line.split(CommonConsts.ID_SEPARATOR, 2);

                if (tok.length == 2) {
                    docMap.put(Integer.valueOf(tok[0]), isRecover ? Long.valueOf(tok[1]) : pos);
                }
                pos += line.length() + System.lineSeparator().length();
            }
            return docMap;
        }
    }

    public static String findLineInFileByIdField(String file, Object field) throws IOException {
        // Iterative — original was recursive and would StackOverflow on large files
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            long pos = 0;
            while (true) {
                raf.seek(pos);
                String line = raf.readLine();
                if (line == null || line.isEmpty()) return null;
                String[] tok = line.split(CommonConsts.ID_SEPARATOR, 2);
                if (tok[0].equals(String.valueOf(field))) {
                    LOGGER.fine("findLineInFileByIdField found: " + line);
                    return line;
                }
                pos += line.length() + System.lineSeparator().length();
            }
        }
    }

    public static String findLineByOffset(String file, long pos) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            raf.seek(pos);
            return raf.readLine();
        }
    }

    public static boolean createDirectoryIfNotExist(String dirPath) {
        File dir = new File(dirPath);
        if (!dir.exists()) {
            LOGGER.info("Creating directory: " + dirPath);
            return dir.mkdirs();
        }
        return false;
    }

    public static boolean isFileExist(String file) {
        return Files.exists(Paths.get(file));
    }

    public static boolean isLineInFileExist(String file, String line) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String current;
            while ((current = br.readLine()) != null) {
                if (current.trim().equals(line)) return true;
            }
        }
        return false;
    }

    public static boolean findLineInFile(String filePath, String lineToFind) throws IOException {
        return isLineInFileExist(filePath, lineToFind);
    }

    public static void writeHashIndexToDisc(Map<Integer, Long> hashIndex, String snapshotFile)
            throws IOException {
        Path path = Paths.get(snapshotFile);
        Files.deleteIfExists(path);
        try (Writer w = Files.newBufferedWriter(path)) {
            for (Map.Entry<Integer, Long> e : hashIndex.entrySet()) {
                w.write(e.getKey() + CommonConsts.HASH_SNAPSHOT_SEPARATOR
                        + e.getValue() + System.lineSeparator());
            }
        }
    }

    public static void deleteFilesWithPattern(String directoryPath, String pattern) throws IOException {
        Path dirPath = Paths.get(directoryPath);
        if (!Files.exists(dirPath) || !Files.isDirectory(dirPath))
            throw new IOException("Not a directory: " + directoryPath);
        Pattern regex = Pattern.compile(pattern);
        Files.walkFileTree(dirPath, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (regex.matcher(file.getFileName().toString()).find()) Files.delete(file);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public static void removeFile(String file) throws IOException {
        Files.delete(Paths.get(file));
    }
}
