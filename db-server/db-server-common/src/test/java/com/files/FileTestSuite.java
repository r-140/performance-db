package com.files;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)

@Suite.SuiteClasses({
        WriteToFileTest.class,
        ReadFromFileTest.class,
        DeleteLineFromFileTest.class
})
public class FileTestSuite {

}
