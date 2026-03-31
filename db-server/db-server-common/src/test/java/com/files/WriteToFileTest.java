package com.files;


import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Disabled
public class WriteToFileTest extends AbstractTest{
    @TempDir
    Path tempDir;

    @Test
    void testWriteHashIndexToDisc() throws IOException {
        Path tempFile = tempDir.resolve("testSnapshot.txt");

        FileHelper.writeHashIndexToDisc(hashIndex, tempFile.toString());

        // Читаем содержимое файла и проверяем его
        List<String> lines = Files.readAllLines(tempFile);
            assertNotNull(lines);
            assertEquals(5, lines.size());
            assertTrue(lines.contains("1:31"));
            assertTrue(lines.contains("2:62"));
            assertTrue(lines.contains("3:93"));
            assertTrue(lines.contains("4:124"));
    }

    @Test
    void testWriteHashIndexToDiscWithExistingFile() throws IOException {
        Path tempFile = tempDir.resolve("testSnapshot.txt");
        Files.write(tempFile, List.of("Old data"));

        FileHelper.writeHashIndexToDisc(hashIndex, tempFile.toString());

        // Читаем содержимое файла и проверяем его
        List<String> lines = Files.readAllLines(tempFile);
            assertNotNull(lines);
            assertEquals(5, lines.size());
            assertTrue(lines.contains("1:31"));
            assertTrue(lines.contains("2:62"));
            assertTrue(lines.contains("3:93"));
            assertTrue(lines.contains("4:124"));
    }

    @Test
    public void writeDataToFileTest() throws IOException {
            Path tempFile = tempDir.resolve("testFile.txt");

//            todo check offset value
            long offset = FileHelper.writeToFile(tempFile.toString(), "0,{\"data\":\"testdata1\",\"id\":0}", true);

            assertEquals(offset, 0);

            offset = FileHelper.writeToFile(tempFile.toString(), "1,{\"data\":\"testdata1\",\"id\":1}", true);
            assertEquals(offset, (29 + System.lineSeparator().length()));

            offset = FileHelper.writeToFile(tempFile.toString(), "2,{\"data\":\"testdata1\",\"id\":2}", true);
            assertEquals(offset, 60 + System.lineSeparator().length());

            offset = FileHelper.writeToFile(tempFile.toString(), "3,{\"data\":\"testdata1\",\"id\":3}", true);
            assertEquals(offset, 91+ System.lineSeparator().length());
            offset = FileHelper.writeToFile(tempFile.toString(), "13,{\"data\":\"testdata1\",\"id\":13}", true);
            assertEquals(offset, 122 + System.lineSeparator().length());
    }
//
}
