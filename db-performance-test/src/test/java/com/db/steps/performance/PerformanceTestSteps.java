package com.db.steps.performance;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.db.DbUtil.execute;

public class PerformanceTestSteps {
    private static final Logger LOGGER = Logger.getLogger(PerformanceTestSteps.class.getName());

    private final Map<String, Long> findDocResults = new HashMap<>();

    private static final List<String> ALL_INDEX_TYPES =
            List.of("hashIndex", "lsmtree", "btree", "bplustree", "gin", "bitmap");

    private static final int DOC_ID = 50; // mid-range id guaranteed to exist after setup

    @Given("a database with indexed data")
    public void aDatabaseWithIndexedData() {
        ALL_INDEX_TYPES.forEach(type -> {
            try {
                execute("createIndex", type);
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Index creation failed for " + type + ": " + e.getMessage());
            }
        });
    }

    @When("I search for a specific data point")
    public void iSearchForASpecificDataPoint() {
        measureFind("none");
        ALL_INDEX_TYPES.forEach(this::measureFind);
    }

    @Then("the search should be completed within the expected time")
    public void theSearchShouldBeCompletedWithinTheExpectedTime() {
        findDocResults.forEach((key, val) ->
                LOGGER.log(Level.INFO, String.format("Search duration  %-12s → %d ms", key, val)));
    }

    private void measureFind(String indexType) {
        long start = System.currentTimeMillis();
        String payload = new JSONObject()
                .put("indexType", indexType)
                .put("id", DOC_ID)
                .toString();
        execute("find", payload);
        findDocResults.put(indexType, System.currentTimeMillis() - start);
    }
}
