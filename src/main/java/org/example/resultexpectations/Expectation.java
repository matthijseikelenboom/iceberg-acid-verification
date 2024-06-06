package org.example.resultexpectations;

import org.example.resultset.ResultSet;

public interface Expectation {
    boolean isSatisfied(ResultSet resultSet);

    default Or or(Expectation expectation) {
        return Or.create(this, expectation);
    }

}
