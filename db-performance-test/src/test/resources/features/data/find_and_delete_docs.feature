Feature: Find and Delete Data

  Scenario: Find non existing documents
    Given find document with id 10000 and indexType "none"
    When the find document request is sent
    Then the result should be null

  Scenario: Find existing documents without index
    Given find document with id 9 and indexType "none"
    When the find document request is sent
    Then the result should contain "testdata1"

  Scenario: Find existing documents with hashIndex
    Given find document with id 9 and indexType "hashIndex"
    When the find document request is sent
    Then the result should contain "testdata1"

  Scenario: Find existing documents with btree index
    Given find document with id 9 and indexType "btree"
    When the find document request is sent
    Then the result should contain "testdata1"

  Scenario: Find existing documents bplustree index
    Given find document with id 9 and indexType "bplustree"
    When the find document request is sent
    Then the result should contain "testdata1"

  Scenario: Find existing documents lsmtree index
    Given find document with id 9 and indexType "lsmtree"
    When the find document request is sent
    Then the result should contain "testdata1"

  Scenario: Find existing documents lsmtree index
    Given find document with id 9 and indexType "lsmtree"
    When the find document request is sent
    Then the result should contain "testdata1"

  Scenario: Delete non existing document
    Given delete document with id 10000
    When the delete document request is sent
    Then the result should be "the document with id 10000 was not found"

  Scenario: Delete existing document
    Given delete document with id 11
    When the delete document request is sent
    Then the result of deletion should be "the document with id 11 has been removed"





