Feature: Create and Delete BPlusTree Index

  Scenario: Create BPlusTree index
    Given creating or deleting index with type "bplustree"
    When the create index request is sent
    Then the output should be "Index with the type bplustree has been created"
#TODO FIX ALL SCENARIOUS BELOW
  Scenario: Create already existing BPlusTree Index
    Given creating or deleting index with type "bplustree"
    When the create index request is sent
    Then the output should be "Index with the type bplustree already exist"

  Scenario: Delete  BPlusTree Index
    Given creating or deleting index with type "bplustree"
    When the create index request is sent
    Then the output should be "Index with the type bplustree has been deleted"

  Scenario: Delete Non Existing BPlusTree Index
    Given creating or deleting index with type "bplustree"
    When the create index request is sent
    Then the output should be "Index with the type bplustree does not exist"




