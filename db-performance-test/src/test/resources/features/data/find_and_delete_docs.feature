Feature: Find and delete documents using various index types

  Scenario Outline: Find existing document with each index type
    Given find document with id 5 and indexType "<indexType>"
    When the find document request is sent
    Then the result should contain "testdata"

    Examples:
      | indexType  |
      | none       |
      | hashIndex  |
      | bplustree  |
      | btree      |
      | lsmtree    |
      | gin        |
      | bitmap     |

  Scenario: Find non-existing document returns null
    Given find document with id 999999 and indexType "none"
    When the find document request is sent
    Then the result should be null

  Scenario: Delete document removes it from all indexes
    Given delete document with id 1
    When the delete document request is sent
    Then the result of deletion should be "1,{\"data\":\"testdata1\",\"id\":1}"
