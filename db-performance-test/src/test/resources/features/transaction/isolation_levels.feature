Feature: Transaction isolation levels and phantom reads

  Transaction isolation controls what data a transaction can see when other
  transactions are concurrently writing. These scenarios use the in-memory
  MVCCStore to demonstrate each anomaly without requiring a live server.

  Note: The MVCCStore and TransactionContext classes live in the indexes module
  and are tested directly via TransactionIsolationTest (unit tests).
  This feature documents the expected behaviour at the feature level.

  Scenario: READ_UNCOMMITTED allows dirty reads
    Given a transaction at isolation level READ_UNCOMMITTED
    And another transaction inserts a document without committing
    When the first transaction reads that document
    Then the dirty data is visible

  Scenario: READ_COMMITTED prevents dirty reads
    Given a transaction at isolation level READ_COMMITTED
    And another transaction inserts a document without committing
    When the first transaction reads that document
    Then the uncommitted data is not visible

  Scenario: REPEATABLE_READ prevents phantom reads on index range scans
    Given a transaction at isolation level REPEATABLE_READ
    And the transaction scans documents with id between 10 and 20
    And another transaction inserts a document with id 15 and commits
    When the first transaction rescans the same range
    Then the second scan returns the same row count as the first

  Scenario: READ_COMMITTED exhibits phantom reads on index range scans
    Given a transaction at isolation level READ_COMMITTED
    And the transaction scans documents with id between 10 and 20
    And another transaction inserts a document with id 15 and commits
    When the first transaction rescans the same range
    Then the second scan returns more rows than the first
