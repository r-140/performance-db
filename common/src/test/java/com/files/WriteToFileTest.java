package com.files;

import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Created by Illia_Ushakov on 4/22/2019.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class WriteToFileTest extends AbstractTest{



//
    @BeforeClass
    public static void clearFileDir(){
        File file = new File(PATH_TO_FILE);
        file.delete();


    }

    @Test
    public void writeDataToFileTest(){
        try {

//            todo check offset value
            long offset = FileHelper.writeToFile(PATH_TO_FILE, "0,{\"data\":\"testdata1\",\"id\":0}", true);

            assertEquals(offset, 0);

            offset = FileHelper.writeToFile(PATH_TO_FILE, "1,{\"data\":\"testdata1\",\"id\":1}", true);
            assertEquals(offset, (29 + System.lineSeparator().length()));

            offset = FileHelper.writeToFile(PATH_TO_FILE, "2,{\"data\":\"testdata1\",\"id\":2}", true);
            assertEquals(offset, 60 + System.lineSeparator().length());

            offset = FileHelper.writeToFile(PATH_TO_FILE, "3,{\"data\":\"testdata1\",\"id\":3}", true);
            assertEquals(offset, 91+ System.lineSeparator().length());
            offset = FileHelper.writeToFile(PATH_TO_FILE, "13,{\"data\":\"testdata1\",\"id\":13}", true);
            assertEquals(offset, 122 + System.lineSeparator().length());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void writeSnapshotHashIndexTest(){
        try {
            FileHelper.writeHashIndexToDisc(hashIndex, SNAPSHOT_FILE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
