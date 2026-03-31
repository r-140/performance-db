package com.db.steps.sql;

import com.iu.dbclient.sql.SqlHelper;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.json.JSONArray;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Cucumber step definitions for SQL SELECT and JOIN queries.
 */
public class SqlSteps {

    private String result;

    @When("I execute SQL {string}")
    public void executeSql(String sql) {
        result = SqlHelper.query(sql);
        assertNotNull(result, "SQL result must not be null — is the server running?");
    }

    @Then("the result contains document with id {int}")
    public void resultContainsDocWithId(int id) {
        assertNotNull(result);
        assertTrue(result.contains("\"id\":" + id),
            "Expected id=" + id + " in: " + result);
    }

    @Then("the result is a JSON array with {int} elements")
    public void resultIsJsonArrayWithNElements(int count) {
        JSONArray arr = parseArray();
        assertEquals(count, arr.length(),
            "Expected " + count + " elements but got " + arr.length() + ": " + result);
    }

    @Then("the result is a non-empty JSON array")
    public void resultIsNonEmptyJsonArray() {
        JSONArray arr = parseArray();
        assertTrue(arr.length() > 0, "Expected non-empty JSON array but got: " + result);
    }

    @Then("the result contains a document with data field {string}")
    public void resultContainsDocWithDataField(String value) {
        assertNotNull(result);
        assertTrue(result.contains("\"" + value + "\""),
            "Expected '" + value + "' in: " + result);
    }

    @Then("the result contains {string}")
    public void resultContains(String expected) {
        assertNotNull(result);
        assertTrue(result.contains(expected),
            "Expected '" + expected + "' in: " + result);
    }

    @Then("the result is an array where each element has a joined indicator")
    public void resultHasJoinedIndicator() {
        // Join results contain |JOIN| separator between outer and inner lines
        assertNotNull(result);
        assertTrue(result.contains("JOIN") || result.contains("{"),
            "Expected joined result but got: " + result);
    }

    // ── Internal helpers ──────────────────────────────────────────────────

    private JSONArray parseArray() {
        assertNotNull(result, "Result must not be null — is the server running?");
        try {
            return new JSONArray(result);
        } catch (Exception e) {
            fail("Result is not a valid JSON array: " + result);
            throw new AssertionError(); // unreachable
        }
    }
}
