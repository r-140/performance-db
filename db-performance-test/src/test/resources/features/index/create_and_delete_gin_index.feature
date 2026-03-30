Feature: Create and delete GIN index

  GIN (Generalized Inverted Index) tokenises document values and maps each
  token to a posting list of file offsets. Ideal for full-text search.

  Scenario: Create GIN index successfully
    Given creating or deleting index with type "gin"
    When the create index request is sent
    Then the output should be "Index with the type gin has been created"

  Scenario: Cannot create GIN index twice
    Given creating or deleting index with type "gin"
    When the create index request is sent
    Then the output should be "Index with the type gin has been created"
    When the create index request is sent
    Then the output should contain "DB-402"

  Scenario: Delete GIN index successfully
    Given creating or deleting index with type "gin"
    When the create index request is sent
    Then the output should be "Index with the type gin has been created"
    When the delete index request is sent
    Then the output should be "Index with the type gin has been deleted"
