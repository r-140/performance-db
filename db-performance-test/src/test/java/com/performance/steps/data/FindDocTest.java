package com.performance.steps.data;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.json.JSONObject;


import static com.performance.DbUtil.execute;
import static org.junit.jupiter.api.Assertions.*;

public class FindDocTest {
    private int id;
    private String indexType;
    private String output;

    @Given("find document with id {int} and indexType {string}")
    public void prepareInputForFind(int input, String indexType) {
        this.id = input;
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
    public void deleteIndexRequestSent() {
        this.output = readOrDeleteDoc(id, "none", "delete");
    }

    @Then("the result should be null")
    public void theResultShouldBeNUll() {
        assertNull(output);
    }

    @Then("the result should be {string}")
    public void theResultShouldBe(String expectedResult) {
        assertEquals(expectedResult, output);
    }

    @Then("the result should contain {string}")
    public void theOutputShouldContain(String expectedOutput) {
        assertNotNull(output);
        assertTrue(output.contains(String.valueOf(id)));
        assertTrue(output.contains(expectedOutput));
    }

    @Then("the result of deletion should be {string}")
    public void theDeletionResultShouldBe(String expectedResult) {
        assertEquals(expectedResult, output);

        assertNull(readOrDeleteDoc(id, "none", "find"));
        assertNull(readOrDeleteDoc(id, "hashIndex", "find"));
        assertNull(readOrDeleteDoc(id, "bplustree", "find"));
        assertNull(readOrDeleteDoc(id, "btree", "find"));
        assertNull(readOrDeleteDoc(id, "lsmtree", "find"));
    }

    private String readOrDeleteDoc(int id, String indexType, String taskType) {
        String payload = buildRequestBody(taskType, id, indexType);

        return execute(taskType, payload);
    }

    private static String buildRequestBody(String taskType, int id, String indexType) {
        return "find".equals(taskType) ? new JSONObject()
                .put("indexType", indexType).put("id", id).toString()
                : new JSONObject().put("id", id).toString();
    }

}
