package org.example.resultexpectations;

import lombok.AllArgsConstructor;
import lombok.ToString;
import org.example.resultset.ResultSet;
import org.example.resultset.Record;

/**
 * Expectation that no record with the given primary key is present in the {@link ResultSet}
 */
@AllArgsConstructor(staticName = "create")
@ToString
public class ExpectRecordAbsence implements Expectation {
    private final Record recordExpectedToBeAbsent;

    @Override
    public boolean isSatisfied(ResultSet resultSet) {
        final var resultSetRecord = resultSet.getRecordByPrimaryKey(recordExpectedToBeAbsent.getPrimaryKeyValue());
        return resultSetRecord.isEmpty();
    }
}
