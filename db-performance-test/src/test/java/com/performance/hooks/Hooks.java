package com.performance.hooks;

import io.cucumber.java.AfterAll;
import io.cucumber.java.BeforeAll;

import static com.performance.DbUtil.generateTestData;
import static com.performance.DbUtil.removeDB;

public class Hooks {
    @BeforeAll
    public void setup(){
        removeDB();
        generateTestData();
    }

    @AfterAll
    public void tearDown(){
        removeDB();
    }
}
