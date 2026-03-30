Feature: Create and delete Bitmap index

  Bitmap indexes use one BitSet per distinct value, enabling fast AND/OR
  set operations. Best suited for low-cardinality columns (status, category).

  Scenario: Create Bitmap index successfully
    Given creating or deleting index with type "bitmap"
    When the create index request is sent
    Then the output should be "Index with the type bitmap has been created"

  Scenario: Cannot create Bitmap index twice
    Given creating or deleting index with type "bitmap"
    When the create index request is sent
    Then the output should be "Index with the type bitmap has been created"
    When the create index request is sent
    Then the output should contain "DB-402"

  Scenario: Delete Bitmap index successfully
    Given creating or deleting index with type "bitmap"
    When the create index request is sent
    Then the output should be "Index with the type bitmap has been created"
    When the delete index request is sent
    Then the output should be "Index with the type bitmap has been deleted"
