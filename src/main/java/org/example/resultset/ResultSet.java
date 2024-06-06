package org.example.resultset;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@EqualsAndHashCode
@ToString(onlyExplicitlyIncluded = true)
public class ResultSet {

    @Getter
    @ToString.Include
    private final List<Record> records;
    private final Map<String, List<Record>> recordByPrimaryKey;

    public ResultSet(final List<Record> records) {
        this.records = records;
        this.recordByPrimaryKey = records.stream().collect(Collectors.groupingBy(Record::getPrimaryKeyValue));
    }

    public Optional<Record> getRecordByPrimaryKey(String primaryKeyValue) {
        var matchingRecords = recordByPrimaryKey.getOrDefault(primaryKeyValue, List.of());
        switch (matchingRecords.size()) {
            case 0:
                return Optional.empty();
            case 1:
                return Optional.of(matchingRecords.get(0));
            default:
                throw new InconsistentResultSetException("More than one row found with primaryKeyValue " + primaryKeyValue + ". Rows found:\n" + matchingRecords);
        }
    }

}
