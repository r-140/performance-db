Feature: Create and Delete BPlusTree Index

  Scenario: Create BPlusTree index
    Given creating index with type "bplustree"
    When the create index request is sent
    Then the output should be "Index with the type bplustree has been created"

#  Scenario: Create already existing BPlusTree Index
#    Given creating "bplustree"
#    When the response obtained
#    Then the output should be "Index with the type bplustree already exist"
#
#  Scenario: Delete BPlusTree Index
#    Given deleting "bplustree"
#    When the response obtained
#    Then the output should be "Index with the type bplustree does not exist"
#
#  Scenario: Delete BPlusTree Index
#    Given deleting non existing "bplustree"
#    When the response obtained
#    Then the output should be "Index with the type bplustree has been deleted"


