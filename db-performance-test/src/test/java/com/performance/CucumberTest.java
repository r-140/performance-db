package com.performance;

import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import org.junit.runner.RunWith;

//@Suite
//@IncludeEngines("cucumber")
//@SelectClasspathResource("features")
@RunWith(Cucumber.class)
@CucumberOptions(
        features = "src/test/resources/features",
        glue = {"com.performance.steps", "com.performance.hooks"}
)
public class CucumberTest {
}

