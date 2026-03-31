Feature: SQL JOIN queries — OLTP join strategies

  The query planner automatically selects the join strategy based on
  available indexes and the estimated outer result size.

  Background:
    Given the database has test data

  Scenario: Hash join when no index on join column
    When I execute SQL "SELECT * FROM data JOIN data ON id = id LIMIT 5"
    Then the result is a non-empty JSON array

  Scenario: Explicit HASH join hint respected
    When I execute SQL "SELECT * FROM data JOIN data ON id = id HASH LIMIT 3"
    Then the result is a non-empty JSON array

  Scenario: Explicit NESTED_LOOP join hint respected
    When I execute SQL "SELECT * FROM data JOIN data ON id = id NESTED_LOOP LIMIT 2"
    Then the result is a non-empty JSON array

  Scenario: Join with WHERE clause filters outer rows first
    Given creating or deleting index with type "hashIndex"
    When the create index request is sent
    Then the output should be "Index with the type hashIndex has been created"
    When I execute SQL "SELECT * FROM data JOIN data ON id = id WHERE id = 5"
    Then the result is a non-empty JSON array
    Then the result contains document with id 5
