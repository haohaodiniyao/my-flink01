package com.example;

import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.util.Preconditions;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @author xjk
 * @date 2020/3/24
 */
public class DayBucketAssigner extends DateTimeBucketAssigner<MsgData> {
    private final String formatString;

    private final ZoneId zoneId = ZoneId.of("Asia/Shanghai");

    private transient DateTimeFormatter dateTimeFormatter;

    public DayBucketAssigner(String formatString) {
        this.formatString = Preconditions.checkNotNull(formatString);
    }

    @Override
    public String getBucketId(MsgData element, Context context) {
        if (dateTimeFormatter == null) {
            dateTimeFormatter = DateTimeFormatter.ofPattern(formatString).withZone(zoneId);
        }
        return "dt=" + dateTimeFormatter.format(Instant.ofEpochMilli(element.getTs()));
    }

}