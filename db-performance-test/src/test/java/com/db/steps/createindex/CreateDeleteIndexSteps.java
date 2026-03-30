package com.db.steps.createindex;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import static com.db.DbUtil.execute;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CreateDeleteIndexSteps {

    private String input;
    private String output;

    @Given("creating or deleting index with type {string}")
    public void prepareInputForIndexOperation(String input) {
        this.input = input;
    }

    @When("the create index request is sent")
    public void createIndexRequestSent() {
        this.output = execute("createIndex", input);
    }

    @When("the delete index request is sent")
    public void deleteIndexRequestSent() {
        this.output = execute("deleteIndex", input);
    }

    @Then("the output should be {string}")
    public void theOutputShouldBe(String expectedOutput) {
        assertEquals(expectedOutput, output);
    }

    @Then("the output should contain {string}")
    public void theOutputShouldContain(String expectedOutput) {
        assertTrue(output != null && output.contains(expectedOutput),
                "Expected output to contain '" + expectedOutput + "' but got: " + output);
    }
}
