package com.db.steps.data;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.json.JSONObject;

import static com.db.DbUtil.execute;
import static org.junit.jupiter.api.Assertions.*;

public class FindDocTest {
    private int id;
    private String indexType;
    private String output;

    @Given("find document with id {int} and indexType {string}")
    public void prepareInputForFind(int input, String indexType) {
        this.id        = input;
        this.indexType = indexType;
    }

    @Given("delete document with id {int}")
    public void prepareInputForDeleteData(int input) {
        this.id = input;
    }

    @When("the find document request is sent")
    public void findDocRequest() {
        this.output = readOrDeleteDoc(id, indexType, "find");
    }

    @When("the delete document request is sent")
    public void deleteDocRequestSent() {
        this.output = readOrDeleteDoc(id, "none", "delete");
    }

    @Then("the result should be null")
    public void theResultShouldBeNull() {
        assertNull(output);
    }

    @Then("the result should be {string}")
    public void theResultShouldBe(String expectedResult) {
        assertEquals(expectedResult, output);
    }

    @Then("the result should contain {string}")
    public void theResultShouldContain(String expectedOutput) {
        assertNotNull(output, "Expected result to contain '" + expectedOutput + "' but got null");
        assertTrue(output.contains(expectedOutput),
                "Expected result to contain '" + expectedOutput + "' but got: " + output);
    }

    @Then("the result of deletion should be {string}")
    public void theDeletionResultShouldBe(String expectedResult) {
        assertEquals(expectedResult, output);

        // Verify document is gone from all index types including gin and bitmap
        for (String idx : new String[]{"none", "hashIndex", "bplustree", "btree", "lsmtree", "gin", "bitmap"}) {
            assertNull(readOrDeleteDoc(id, idx, "find"),
                    "Document should be absent when searched with index: " + idx);
        }
    }

    private String readOrDeleteDoc(int id, String indexType, String taskType) {
        String payload = "find".equals(taskType)
                ? new JSONObject().put("indexType", indexType).put("id", id).toString()
                : new JSONObject().put("id", id).toString();
        return execute(taskType, payload);
    }
}
