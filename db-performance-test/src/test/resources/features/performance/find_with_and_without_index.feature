Feature: Performance comparison across all index types

  Compares search time for a single document lookup with no index (full
  file scan) versus each of the six index types. All timings are logged
  at INFO level; no hard time limit is enforced so the test passes on
  any hardware.

  Scenario: Compare search duration across all index types
    Given a database with indexed data
    When I search for a specific data point
    Then the search should be completed within the expected time
