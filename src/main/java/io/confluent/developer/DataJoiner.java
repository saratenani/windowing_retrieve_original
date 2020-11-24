package io.confluent.developer;
import org.apache.kafka.streams.kstream.ValueJoiner;

import io.confluent.demo.Inputdatat;
import io.confluent.demo.Anomaly;
import io.confluent.demo.Joined;

public class DataJoiner implements ValueJoiner<Inputdatat,Anomaly,Joined>{

    public Joined apply(Inputdatat data, Anomaly ids){
        return Joined.newBuilder()
                .setKey(ids.getId())
                .setTimestamp(data.getTimestamp())
                .setVal(data.getVal())
                .setWindowStart(ids.getWindowStart())
                .setWindowEnd(ids.getWindowEnd())
                .build();
    }
}
