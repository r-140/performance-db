package com.performance.steps.createindex;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import static com.performance.DbUtil.execute;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class CreateDeleteIndexSteps {

    private String input;
    private String output;

    @Given("creating or deleting index with type {string}")
    public void prepareInputForIndexOperation(String input) {
        this.input = input;
    }

    @When("the create index request is sent")
    public void createIndexRequestSent() {
        this.output = createOrDeleteIndex(input, "createIndex");
    }

    @When("the delete index request is sent")
    public void deleteIndexRequestSent() {
        this.output = createOrDeleteIndex(input, "deleteIndex");
    }

    @Then("the output should be {string}")
    public void theOutputShouldBe(String expectedOutput) {
        assertEquals(expectedOutput, output);
    }

    private String createOrDeleteIndex(String indexType, String taskType) {
        return execute(taskType, indexType);
    }
}
