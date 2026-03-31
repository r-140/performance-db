package com.files;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

/**
 * Created by Illia_Ushakov on 4/22/2019.
 */
@Disabled
public class ReadFromFileTest extends AbstractTest{
//
    @Test
    public void readSnapshotFileTest(){
        try {
            Map<Integer, Long> map = FileHelper.readFile(SNAPSHOT_FILE, true);
            Assertions.assertEquals(map, hashIndex);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void readDataFileTest(){
        try {
            Map<Integer, Long> map = FileHelper.readFile(PATH_TO_FILE, false);
            Assertions.assertEquals(map, hashIndex);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void findLineInFileByFieldTest(){
        final String id = "3";

        try {
            final String result = FileHelper.findLineInFileByIdField(PATH_TO_FILE, id);
            Assertions.assertEquals(result, "3,{\"data\":\"testdata1\",\"id\":3}");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void findLineByOffsetTest(){
        try {
            final String result = FileHelper.findLineByOffset(PATH_TO_FILE, 91+ System.lineSeparator().length());

            Assertions.assertEquals(result, "3,{\"data\":\"testdata1\",\"id\":3}");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void isFileExistTest(){
        boolean isFileExist = FileHelper.isFileExist(PATH_TO_FILE);
        Assertions.assertEquals(isFileExist, true);
    }

    @Test
    public void isLineInFileExistTest(){
        try {
            boolean isLineInFileExist = FileHelper.isLineInFileExist(PATH_TO_FILE, "3,{\"data\":\"testdata1\",\"id\":3}");
            Assertions.assertEquals(isLineInFileExist, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
