package com.db.steps.data;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.json.JSONObject;

import static com.db.DbUtil.execute;
import static org.junit.jupiter.api.Assertions.*;

public class AddDocTest {
    private int id;
    private String output;

    @Given("add document with id {int}")
    public void add_document_with_id(int input) {
        this.id = input;
    }

    @When("the add document request is sent")
    public void the_add_document_request_is_sent() {
        this.output = appendDoc(id);
    }


    @Then("the response should be {string}")
    public void the_response_should_be(String expectedResult) {
        assertEquals(expectedResult, output);
    }

    private String appendDoc(int id) {

        String payload = new JSONObject()
                .put("data", "testdata" + id)
                .put("id", id).toString();

        return execute("append", payload);
    }

}
