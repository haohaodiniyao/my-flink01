package com.example;

import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

public class MySink {
    public static StreamingFileSink<MsgData> newStreamingFileSink(String sinkPath) {
        return StreamingFileSink
                .forBulkFormat(new Path(sinkPath), ParquetAvroWriters.forReflectRecord(MsgData.class))
//                .forRowFormat(new Path(sinkPath),new SimpleStringEncoder<MsgData>("UTF-8"))
                .withBucketAssigner(new DayBucketAssigner("yyyyMMdd"))
                .withBucketCheckInterval(1000)
                .build();
    }
}
