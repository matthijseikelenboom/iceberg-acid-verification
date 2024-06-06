package org.example.resultexpectations;

import org.example.resultset.ResultSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ResultSetExpectations {
    private final Map<String, Expectation> expectationPerPrimaryKeyValue = new HashMap<>();

    public void setRecordExpectation(String primaryKeyValue, Expectation expectation) {
        expectationPerPrimaryKeyValue.put(primaryKeyValue, expectation);
    }

    public Optional<Expectation> getRecordExpectation(String primaryKeyValue) {
        return Optional.ofNullable(expectationPerPrimaryKeyValue.get(primaryKeyValue));
    }

    public boolean isStatisfied(ResultSet resultSet) {
        var satisfied = true;

        for (Expectation expectation : expectationPerPrimaryKeyValue.values()) {
            var expectationStatisfied = expectation.isSatisfied(resultSet);
            if (!expectationStatisfied) {
                System.err.println("Expectation not satisfied: " + expectation);
                satisfied = false;
            }
        }

        // Check no other primary key value
        var primaryKeyValuesWithExpectations = expectationPerPrimaryKeyValue.keySet();
        for (var r : resultSet.getRecords()) {
            if (!primaryKeyValuesWithExpectations.contains(r.getPrimaryKeyValue())) {
                System.err.println("Unexpected primary key in the result set: " + r.getPrimaryKeyValue());
                satisfied = false;
            }
        }

        return satisfied;
    }

}
