package org.example.resultexpectations;

import lombok.AllArgsConstructor;
import lombok.ToString;
import org.example.resultset.ResultSet;

@AllArgsConstructor(staticName = "create")
@ToString
public class Or implements Expectation {

    private final Expectation left;
    private final Expectation right;

    @Override
    public boolean isSatisfied(ResultSet resultSet) {
        return left.isSatisfied(resultSet) || right.isSatisfied(resultSet);
    }
}
