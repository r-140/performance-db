Feature: Create and Delete LSMTree Index

  Scenario: Create LSMTree index
    Given creating index with type "lsmtree"
    When the create index request is sent
    Then the output should be "Index with the type lsmtree has been created"

#  Scenario: Create already existing LSMTree Index
#    Given creating "lsmtree"
#    When the response obtained
#    Then the output should be "Index with the type lsmtree already exist"
#
#  Scenario: Delete LSMTree Index
#    Given deleting "lsmtree"
#    When the response obtained
#    Then the output should be "Index with the type lsmtree does not exist"
#
#  Scenario: Delete LSMTree Index
#    Given deleting non existing "lsmtree"
#    When the response obtained
#    Then the output should be "Index with the type lsmtree has been deleted"


