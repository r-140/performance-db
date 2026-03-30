package com.files;

import com.util.CommonConsts;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Disabled
class FileHelperTest {

    @TempDir
    Path tempDir;

    @Test
    void testRemoveLineFromFile() throws IOException {
        Path tempFile = tempDir.resolve("testFile.txt");
        Files.write(tempFile, "Line1\nLineToRemove\nLine3\n".getBytes());

        FileHelper.removeLineFromFile(tempFile.toString(), "LineToRemove");

        String content = Files.readString(tempFile);
        assertFalse(content.contains("LineToRemove"));
        assertTrue(content.contains("Line1"));
        assertTrue(content.contains("Line3"));
    }

    @Test
    void testWriteToFile() throws IOException {
        Path tempFile = tempDir.resolve("testFile.txt");

        FileHelper.writeToFile(tempFile.toString(), "TestData", false);
        String content = Files.readString(tempFile);
        assertEquals("TestData" + System.lineSeparator(), content);

        FileHelper.writeToFile(tempFile.toString(), "MoreData", true);
        content = Files.readString(tempFile);
        assertEquals("TestData" + System.lineSeparator() + "MoreData" + System.lineSeparator(), content);
    }

    @Test
    void testReadFile() throws IOException {
        Path tempFile = tempDir.resolve("testFile.txt");
        Files.write(tempFile, ("1" + CommonConsts.ID_SEPARATOR + "100\n2" + CommonConsts.ID_SEPARATOR + "200\n").getBytes());

        Map<Integer, Long> result = FileHelper.readFile(tempFile.toString(), false);
        assertEquals(2, result.size());
        assertTrue(result.containsKey(1));
        assertTrue(result.containsKey(2));
    }

    @Test
    void testFindLineInFileByIdField() throws IOException {
        Path tempFile = tempDir.resolve("testFile.txt");
        Files.write(tempFile, ("1" + CommonConsts.ID_SEPARATOR + "100\n" + "2" + CommonConsts.ID_SEPARATOR + "200\n").getBytes());

        String line = FileHelper.findLineInFileByIdField(tempFile.toString(), 2);
        assertNotNull(line);
        assertEquals("2" + CommonConsts.ID_SEPARATOR + "200", line);
    }

    @Test
    void testFindLineByOffset() throws IOException {
        Path tempFile = tempDir.resolve("testFile.txt");
        Files.write(tempFile, ("Line1\nLine2\nLine3\n").getBytes());

        String line = FileHelper.findLineByOffset(tempFile.toString(), 6);
        assertEquals("Line2", line);
    }

    @Test
    void testCreateDirectoryIfNotExist() {
        Path tempDirPath = tempDir.resolve("newDir");

        boolean created = FileHelper.createDirectoryIfNotExist(tempDirPath.toString());
        assertTrue(created);
        assertTrue(Files.exists(tempDirPath));

        boolean notCreated = FileHelper.createDirectoryIfNotExist(tempDirPath.toString());
        assertFalse(notCreated);
    }

    @Test
    void testIsFileExist() throws IOException {
        Path tempFile = tempDir.resolve("testFile.txt");
        Files.createFile(tempFile);

        assertTrue(FileHelper.isFileExist(tempFile.toString()));
        assertFalse(FileHelper.isFileExist(tempDir.resolve("nonExistentFile.txt").toString()));
    }

    @Test
    void testIsLineInFileExist() throws IOException {
        Path tempFile = tempDir.resolve("testFile.txt");
        Files.write(tempFile, ("Line1\nLine2\nLine3\n").getBytes());

        assertTrue(FileHelper.isLineInFileExist(tempFile.toString(), "Line2"));
        assertFalse(FileHelper.isLineInFileExist(tempFile.toString(), "Line4"));
    }

    @Test
    void testWriteHashIndexToDisc() throws IOException {
        Path tempFile = tempDir.resolve("hashIndex.txt");

        Map<Integer, Long> hashIndex = new HashMap<>();
        hashIndex.put(1, 100L);
        hashIndex.put(2, 200L);

        FileHelper.writeHashIndexToDisc(hashIndex, tempFile.toString());

        String content = Files.readString(tempFile);
        assertTrue(content.contains("1" + CommonConsts.HASH_SNAPSHOT_SEPARATOR + "100"));
        assertTrue(content.contains("2" + CommonConsts.HASH_SNAPSHOT_SEPARATOR + "200"));
    }
}

