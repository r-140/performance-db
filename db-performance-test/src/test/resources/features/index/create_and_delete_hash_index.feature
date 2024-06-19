Feature: Create and Delete hash index Index

  Scenario: Create Hash Index
    Given creating or deleting index with type "hashIndex"
    When the create index request is sent
    Then the output should be "Index with the type hashIndex has been created"
#TODO FIX ALL SCENARIOUS BELOW
  Scenario: Create already existing Hash Index
    Given creating or deleting index with type "hashIndex"
    When the create index request is sent
    Then the output should be "Index with the type hashIndex already exist"

  Scenario: Delete Hash Index
    Given creating or deleting index with type "hashIndex"
    When the delete index request is sent
    Then the output should be "Index with the type hashIndex has been deleted"

  Scenario: Delete Non Existing Hash Index
    Given creating or deleting index with type "hashIndex"
    When the delete index request is sent
    Then the output should be "Index with the type hashIndex does not exist"



