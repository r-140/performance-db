Feature: Append Document

  Scenario: Add new document to db
    Given add document with id 1
    When the add document request is sent
    Then the response should be "Document with id 1 has been created"






