Feature: Create and Delete BTree Index

  Scenario: Create BTree index
    Given creating or deleting index with type "btree"
    When the create index request is sent
    Then the output should be "Index with the type btree has been created"

  Scenario: Create already existing BTree Index
    Given creating or deleting index with type "btree"
    When the create index request is sent
    Then the output should be "Index with the type btree already exist"

  Scenario: Delete BTree Index
    Given creating or deleting index with type "btree"
    When the delete index request is sent
    Then the output should be "Index with the type btree has been deleted"

  Scenario: Delete Non Existing BTree Index
    Given creating or deleting index with type "btree"
    When the delete index request is sent
    Then the output should be "Index with the type btree does not exist"





