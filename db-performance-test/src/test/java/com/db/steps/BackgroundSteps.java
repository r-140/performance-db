package com.db.steps;

import io.cucumber.java.en.Given;

import static com.db.DbUtil.generateTestData;

/**
 * Steps used in Background blocks across multiple feature files.
 */
public class BackgroundSteps {

    @Given("the database has test data")
    public void theDatabaseHasTestData() {
        // Data is seeded in Hooks.setup() — this step is a no-op assertion
        // that makes feature files self-documenting.
    }
}
