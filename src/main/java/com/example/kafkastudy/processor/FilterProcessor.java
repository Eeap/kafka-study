package com.example.kafkastudy.processor;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class FilterProcessor implements Processor<String,String,String,String> {
    private ProcessorContext context;
    @Override
    public void process(Record record) {
        if (record.value().toString().length() > 5){
            context.forward(record);
        }
    }

    @Override
    public void init(ProcessorContext context) {
        this.context=context;
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}
