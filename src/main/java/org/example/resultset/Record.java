package org.example.resultset;

import lombok.*;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

import java.io.Serializable;

@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString
@Getter
@Setter
public class Record implements Serializable {
    @NonNull
    @EqualsAndHashCode.Include
    private String primaryKeyValue;

    @NonNull
    @EqualsAndHashCode.Include
    private String partitionKeyValue;

    @EqualsAndHashCode.Include
    private String dataValue;

    public static Encoder<Record> getEncoder() {
        return Encoders.bean(Record.class);
    }

}
