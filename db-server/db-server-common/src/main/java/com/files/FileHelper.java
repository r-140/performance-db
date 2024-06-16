package com.files;

import com.util.CommonConsts;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FileHelper {
    private static final Logger LOGGER = Logger.getLogger(FileHelper.class.getName());

    public static void removeLineFromFile(String filePath, String lineToRemove) throws IOException {
        File inputFile = new File(filePath);
        File tempFile = new File(inputFile.getAbsolutePath() + ".tmp");

        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile));
             BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {

            String currentLine;
            while ((currentLine = reader.readLine()) != null) {
                // Trim newline when comparing with lineToRemove
                String trimmedLine = currentLine.trim();
                if (trimmedLine.equals(lineToRemove)) {
                    continue;
                }
                writer.write(currentLine + System.lineSeparator());
            }
        }

        // Delete the original file
        if (!inputFile.delete()) {
            LOGGER.info("Could not delete file");
            return;
        }

        // Rename the new file to the filename the original file had
        if (!tempFile.renameTo(inputFile)) {
            System.out.println("Could not rename file");
        }
    }


    public static long writeToFile(final String pathToFile, final String data, boolean isAppend) throws IOException {
        return usingFileOutputStream(pathToFile, data, isAppend);
    }

    public static Map<Integer, Long> readFile(String file, boolean isRecover) throws IOException {
        LOGGER.log(Level.FINEST, String.format("readFile(): file %s, isRecover ? %s", file, isRecover));
        RandomAccessFile aFile = new RandomAccessFile(file, "rw");
        final Map<Integer, Long> docMap = isRecover ? readHashSnapshotFileRecursively(new HashMap<>(), aFile, 0) : readFileRecursively(new ConcurrentHashMap<>(), aFile, 0);
        aFile.close();
        LOGGER.log(Level.FINE, String.format("readFile(): Map after reading %s", docMap));

        return docMap;
    }

    public static String findLineInFileByIdField(String file, Object field) throws IOException {
        RandomAccessFile aFile = new RandomAccessFile(file, "rw");
        final String found = findLineByIdRec(aFile, field, 0);
        aFile.close();
        LOGGER.info(String.format("findLineInFileByField(): found %S", found));

        return found;
    }

    public static String findLineByOffset(String file, long pos) throws IOException {
        RandomAccessFile aFile = new RandomAccessFile(file, "rw");
        aFile.seek(pos);
        String line = aFile.readLine();

        LOGGER.fine(String.format("found line " + line));

        return line;
    }

    public static boolean createDirectoryIfNotExist(String dirPath) {
        File directory = new File(dirPath);
        if (!directory.exists()) {
            LOGGER.info(String.format("Directory %s has been created", dirPath));
            return directory.mkdirs();
        }

        LOGGER.info(String.format("Directory %s already exist", dirPath));

        return false;
    }

    public static boolean isFileExist(String file) {
        final Path path = Paths.get(file);
        return Files.exists(path);
    }

    public static boolean isLineInFileExist(String file, String line) throws IOException {
        RandomAccessFile aFile = new RandomAccessFile(file, "rw");

        return isLineExist(aFile, line, 0);
    }

    //    used to be synchronized, now ReadWriteLock is being used
    public static void writeHashIndexToDisc(Map<Integer, Long> hashIndex, String snapshotFile) throws IOException {
        final Path path = Paths.get(snapshotFile);
//        deleting existing file
        boolean result = Files.deleteIfExists(path);
        if (!result)
            LOGGER.log(Level.INFO, "existing file wasn't remove, because it does not exist");

//        writing snapshot to new file
        try (Writer writer = Files.newBufferedWriter(path)) {
            hashIndex.forEach((key, value) -> {
                try {
                    writer.write(key + CommonConsts.HASH_SNAPSHOT_SEPARATOR + value + System.lineSeparator());
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            });
        } catch (UncheckedIOException ex) {
            throw ex.getCause();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void removeFile(String file) throws IOException {
        Path path = Paths.get(file);
        Files.delete(path);
    }

    private static Map<Integer, Long> readHashSnapshotFileRecursively(Map<Integer, Long> docMap, RandomAccessFile aFile, long pos) throws IOException {
        aFile.seek(pos);
        final String line = aFile.readLine();
        if (line == null || line.isEmpty())
            return docMap;

        String[] tok = line.split(CommonConsts.HASH_SNAPSHOT_SEPARATOR, 2);
//
        docMap.put(Integer.valueOf(tok[0]), Long.valueOf(tok[1]));
        pos = pos + line.length() + System.lineSeparator().length();

        return readHashSnapshotFileRecursively(docMap, aFile, pos);
    }

    //    used to be synchronized, now ReadWriteLock is being used
    private static long usingFileOutputStream(String pathToFile, String data, boolean isAppend) throws IOException {
        data = writeNewLineBreakers(data);

        final FileOutputStream outputStream = new FileOutputStream(pathToFile, isAppend);
        final long offset = outputStream.getChannel().size();

        byte[] strToBytes = data.getBytes();
        outputStream.write(strToBytes);

        outputStream.close();

        return offset;
    }

    //    method has to be synchronized otherwise it will be deadlock
    private static String writeNewLineBreakers(String data) {
        //remove all line breakers
        data = data.replaceAll(System.lineSeparator(), "");
        //        write new line breaker at the end of line

//        builder.append("\n");
        return data + System.lineSeparator();
    }


    private static Map<Integer, Long> readFileRecursively(Map<Integer, Long> docMap, RandomAccessFile aFile, long pos) throws IOException {
        aFile.seek(pos);
        String line = aFile.readLine();
        if (line == null || line.isEmpty())
            return docMap;

        final String[] tok = line.split(CommonConsts.ID_SEPARATOR, 2);

        LOGGER.log(Level.FINEST, String.format("id form file %s, corresponding json %s", tok[0], tok[1]));

        docMap.put(Integer.valueOf(tok[0]), pos);
        pos = pos + line.length() + System.lineSeparator().length();

//        pos = pos + line.length();

        return readFileRecursively(docMap, aFile, pos);
    }

    private static String findLineByIdRec(RandomAccessFile aFile, Object field, long pos) throws IOException {
        aFile.seek(pos);
        String line = aFile.readLine();
        if (line == null || line.isEmpty())
            return line;

        final String[] tok = line.split(CommonConsts.ID_SEPARATOR, 2);

        LOGGER.log(Level.FINEST, String.format("id form file %s, corresponding json %s", tok[0], tok[1]));
        if (tok[0].equals(String.valueOf(field)))
            return line;

        pos = pos + line.length() + System.lineSeparator().length();
        return findLineByIdRec(aFile, field, pos);
    }

    private static boolean isLineExist(RandomAccessFile aFile, Object field, long pos) throws IOException {
        aFile.seek(pos);
        String line = aFile.readLine();
        if (line == null || line.isEmpty())
            return false;


        if (line.equals(field))
            return true;

        pos = pos + line.length() + System.lineSeparator().length();
        return isLineExist(aFile, field, pos);
    }

}
