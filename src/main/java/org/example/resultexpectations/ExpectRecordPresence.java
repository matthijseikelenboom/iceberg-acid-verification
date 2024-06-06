package org.example.resultexpectations;

import lombok.AllArgsConstructor;
import lombok.ToString;
import org.example.resultset.Record;
import org.example.resultset.ResultSet;

import java.util.Objects;

/**
 * Expectation that one record with the given primary key is present in the {@link ResultSet} and that the found
 * record exactly a matched the provided {@link Record}
 */
@AllArgsConstructor(staticName = "create")
@ToString
public class ExpectRecordPresence implements Expectation {
    private final Record recordExpectedToBePresent;

    @Override
    public boolean isSatisfied(ResultSet resultSet) {
        final var resultSetRecord = resultSet.getRecordByPrimaryKey(recordExpectedToBePresent.getPrimaryKeyValue()).orElse(null);
        return Objects.equals(recordExpectedToBePresent, resultSetRecord);
    }
}
