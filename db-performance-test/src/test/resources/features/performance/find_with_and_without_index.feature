Feature: Performance testing of data search with and without index

  Scenario: Search data with index
    Given a database with indexed data
    When I search for a specific data point
    Then the search should be completed within the expected time


