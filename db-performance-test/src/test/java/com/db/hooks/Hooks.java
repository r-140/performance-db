package com.db.hooks;

import io.cucumber.java.AfterAll;
import io.cucumber.java.BeforeAll;

import static com.db.DbUtil.generateTestData;
import static com.db.DbUtil.removeDB;

public class Hooks {
    @BeforeAll
    public static void setup() {
        removeDB();          // ensure clean state before every suite run
        generateTestData(100);
    }

    @AfterAll
    public static void tearDown() {
        removeDB();
    }
}
