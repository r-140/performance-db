package com.db.steps.performance;

import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.db.DbUtil.*;

public class PerformanceTestSteps {
    private static final Logger LOGGER = Logger.getLogger(PerformanceTestSteps.class.getName());

    private final  Map<String, Long> findDocResults = new HashMap<>();

    private static final String CREATE_INDEX_TASK = "createIndex";

    private static final String FIND_DOC_TASK = "find";

    private static final List<String> INDEXES = List.of("hashIndex", "lsmtree", "btree", "bplustree");

    private static final int DOC_ID = 914959;

//    @Before
//    public void setUp() throws Exception {
//        generateTestData(1000000);
//    }

//    @After
//    public void tearDown(){
//        removeDB();
//    }

    @Given("a database with indexed data")
    public void aDatabaseWithIndexedData() throws Exception {
        INDEXES.forEach(this::createIndex);
    }

    @When("I search for a specific data point")
    public void iSearchForASpecificDataPoint() throws Exception {
        findDoc("none");

        INDEXES.forEach(this::findDoc);
    }

    @Then("the search should be completed within the expected time")
    public void theSearchShouldBeCompletedWithinTheExpectedTime() {
        findDocResults.forEach((key, val) -> LOGGER.log(Level.INFO, "Search duration: for index " + key + " is " + val + " ms"));
    }

    private void createIndex(String indexType) {
        try {
            execute(CREATE_INDEX_TASK, indexType);
        } catch (Exception e) {
            LOGGER.log(Level.INFO, "exception thrown during index creation " + e.getMessage());
        }
    }

    private void findDoc(String indexType) {
        long startTime = System.currentTimeMillis();
        String payload = new JSONObject()
                .put("indexType", indexType).put("id", PerformanceTestSteps.DOC_ID).toString();
         execute(FIND_DOC_TASK, payload);
        long endTime = System.currentTimeMillis();

        long duration = endTime - startTime;

        findDocResults.put(indexType, duration);
    }

}

