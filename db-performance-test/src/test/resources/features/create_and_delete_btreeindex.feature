Feature: Create and Delete BTree Index

  Scenario: Create BTree index
    Given creating index with type "btree"
    When the create index request is sent
    Then the output should be "Index with the type btree has been created"

#  Scenario: Create already existing BTree Index
#    Given creating "btree"
#    When the response obtained
#    Then the output should be "Index with the type btree already exist"
#
#  Scenario: Delete BTree Index
#    Given deleting "btree"
#    When the response obtained
#    Then the output should be "Index with the type btree does not exist"
#
#  Scenario: Delete BTree Index
#    Given deleting non existing "btree"
#    When the response obtained
#    Then the output should be "Index with the type btree has been deleted"



