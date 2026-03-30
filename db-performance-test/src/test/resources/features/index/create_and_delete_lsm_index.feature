Feature: Create and Delete LSMTree Index
  The LSM index uses Bloom filters on each SSTable to avoid unnecessary
  disk reads when looking up keys that are absent from that file.

  Scenario: Create LSMTree index successfully
    Given creating or deleting index with type "lsmtree"
    When the create index request is sent
    Then the output should be "Index with the type lsmtree has been created"

  Scenario: Cannot create LSMTree index twice
    Given creating or deleting index with type "lsmtree"
    When the create index request is sent
    Then the output should be "Index with the type lsmtree has been created"
    When the create index request is sent
    Then the output should contain "DB-402"

  Scenario: Delete LSMTree index
    Given creating or deleting index with type "lsmtree"
    When the create index request is sent
    Then the output should be "Index with the type lsmtree has been created"
    When the delete index request is sent
    Then the output should be "Index with the type lsmtree has been deleted"

  Scenario: Cannot delete non-existing LSMTree index
    Given creating or deleting index with type "lsmtree"
    When the delete index request is sent
    Then the output should contain "DB-403"
