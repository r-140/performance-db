package com.db;

import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(
        features = "src/test/resources/features",
        glue     = {"com.db.steps", "com.db.hooks"},
        plugin   = {"pretty", "html:target/cucumber-reports.html"}
)
public class CucumberTest {}
