package com.performance.createindex;

import com.iu.dbclient.DBConnection;
import com.iu.dbclient.DbConnector;
import com.message.MessageBean;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CreateIndexSteps {

    private String input;
    private String output;

    @Given("creating or deleting index with type {string}")
    public void prepareInputForHashIndexCreation(String input) {
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


        private static String createOrDeleteIndex(String indexType, String taskType) {

        DBConnection connection= null;
            try {
                connection = DbConnector.INSTANCE.getConnection("localhost", 5555);

                return connection.createIndex(new MessageBean(taskType, indexType));

            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            } finally {
                if (connection!= null)
                    connection.close();
            }
            return null;
        }
}
