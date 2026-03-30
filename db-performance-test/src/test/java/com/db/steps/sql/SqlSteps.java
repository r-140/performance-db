package com.db.steps.sql;

import com.iu.dbclient.sql.SqlHelper;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.json.JSONArray;

import static org.junit.jupiter.api.Assertions.*;

public class SqlSteps {

    private String result;

    @When("I execute SQL {string}")
    public void executeSql(String sql) {
        result = SqlHelper.query(sql);
        assertNotNull(result, "SQL result must not be null");
    }

    @Then("the result contains document with id {int}")
    public void resultContainsDocWithId(int id) {
        assertNotNull(result);
        assertTrue(result.contains("\"id\":" + id),
            "Expected id=" + id + " in result: " + result);
    }

    @Then("the result is a JSON array with {int} elements")
    public void resultIsJsonArrayWithElements(int count) {
        try {
            JSONArray arr = new JSONArray(result);
            assertEquals(count, arr.length(),
                "Expected " + count + " elements but got " + arr.length());
        } catch (Exception e) {
            fail("Result is not valid JSON array: " + result);
        }
    }

    @Then("the result is a non-empty JSON array")
    public void resultIsNonEmptyJsonArray() {
        try {
            JSONArray arr = new JSONArray(result);
            assertTrue(arr.length() > 0, "Expected non-empty JSON array");
        } catch (Exception e) {
            fail("Result is not valid JSON array: " + result);
        }
    }

    @Then("the result contains a document with data field {string}")
    public void resultContainsDocWithDataField(String value) {
        assertNotNull(result);
        assertTrue(result.contains("\"" + value + "\""),
            "Expected data value '" + value + "' in: " + result);
    }

    @Then("the result contains {string}")
    public void resultContains(String expected) {
        assertNotNull(result);
        assertTrue(result.contains(expected),
            "Expected '" + expected + "' in result: " + result);
    }
}
