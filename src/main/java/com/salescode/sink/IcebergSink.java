package com.salescode.sink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Map;

public class IcebergSink extends RichSinkFunction<Map<String, Object>> {

    public IcebergSink(int i) {
    }

    @Override
    public void invoke(Map<String, Object> value, Context context) {

    }
}
