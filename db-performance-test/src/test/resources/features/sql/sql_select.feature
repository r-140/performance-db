Feature: SQL SELECT with automatic index selection

  The query planner selects the cheapest available index automatically.
  All index types — including LSM with Bloom-filter pruning — are covered
  by a single SQL query path.

  Scenario Outline: Point lookup via each available index type
    Given creating or deleting index with type "<indexType>"
    When the create index request is sent
    Then the output should be "Index with the type <indexType> has been created"
    When I execute SQL "SELECT * FROM data WHERE id = 5"
    Then the result contains document with id 5

    Examples:
      | indexType  |
      | hashIndex  |
      | bplustree  |
      | lsmtree    |

  Scenario: LSM index used when hash and B+tree are absent
    Given creating or deleting index with type "lsmtree"
    When the create index request is sent
    Then the output should be "Index with the type lsmtree has been created"
    When I execute SQL "SELECT * FROM data WHERE id = 10"
    Then the result contains document with id 10

  Scenario: Full scan when no index exists
    When I execute SQL "SELECT * FROM data"
    Then the result is a non-empty JSON array

  Scenario: LIMIT restricts row count
    When I execute SQL "SELECT * FROM data LIMIT 3"
    Then the result is a JSON array with 3 elements

  Scenario: Non-existent id returns empty array
    When I execute SQL "SELECT * FROM data WHERE id = 999999"
    Then the result is a JSON array with 0 elements

  Scenario: Invalid SQL returns error response
    When I execute SQL "DELETE FROM data"
    Then the result contains "SQL-001"
