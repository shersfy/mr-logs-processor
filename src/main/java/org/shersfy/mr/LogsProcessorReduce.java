package org.shersfy.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogsProcessorReduce extends Reducer<Text, LongWritable, Text, LongWritable>{

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values,
                          Reducer<Text, LongWritable, Text, LongWritable>.Context context)
                              throws IOException, InterruptedException {
        
        int total = 0;
        while(values.iterator().hasNext()) {
            total += values.iterator().next().get();
        }
        context.write(key, new LongWritable(total));

        logger.info("key={}, total={}", key, total);
    }



}
