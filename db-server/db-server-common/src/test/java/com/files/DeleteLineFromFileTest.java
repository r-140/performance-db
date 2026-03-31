package com.files;

import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@Ignore
public class DeleteLineFromFileTest extends AbstractTest{
    @TempDir
    Path tempDir;

    @Test
    public void testRemoveLine() throws IOException {
        // Создаем временный файл
        Path tempFile = tempDir.resolve("testFile.txt");
        Files.write(tempFile, List.of(
                "This is line 1",
                "This is the line to remove",
                "This is line 3"
        ));

        // Вызываем метод removeLine
        FileHelper.removeLineFromFile(tempFile.toString(), "This is the line to remove");

        // Читаем содержимое файла и проверяем, что строка удалена
        List<String> lines = Files.readAllLines(tempFile);
        assertEquals(2, lines.size());
        assertFalse(lines.contains("This is the line to remove"));
        assertTrue(lines.contains("This is line 1"));
        assertTrue(lines.contains("This is line 3"));
    }

    @Test
    public void testRemoveNonExistingLine() throws IOException {
        // Создаем временный файл
        Path tempFile = tempDir.resolve("testFile.txt");
        Files.write(tempFile, List.of(
                "This is line 1",
                "This is line 2",
                "This is line 3"
        ));

        // Вызываем метод removeLine
        FileHelper.removeLineFromFile(tempFile.toString(), "This line does not exist");

        // Читаем содержимое файла и проверяем, что ничего не изменилось
        List<String> lines = Files.readAllLines(tempFile);
        assertEquals(3, lines.size());
        assertTrue(lines.contains("This is line 1"));
        assertTrue(lines.contains("This is line 2"));
        assertTrue(lines.contains("This is line 3"));
    }
}

