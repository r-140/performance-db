Feature: Create and Delete LSMTree Index

  Scenario: Create LSMTree index
    Given creating or deleting index with type "lsmtree"
    When the create index request is sent
    Then the output should be "Index with the type lsmtree has been created"
#TODO FIX ALL SCENARIOUS BELOW
  Scenario: Create already existing LSMTree Index
    Given creating or deleting index with type "lsmtree"
    When the create index request is sent
    Then the output should be "Index with the type lsmtree already exist"

  Scenario: Delete LSMTree Index
    Given creating or deleting index with type "lsmtree"
    When the delete index request is sent
    Then the output should be "Index with the type lsmtree has been deleted"

  Scenario: Delete Non Existing LSMTree Index
    Given creating or deleting index with type "lsmtree"
    When the delete index request is sent
    Then the output should be "Index with the type lsmtree does not exist"




