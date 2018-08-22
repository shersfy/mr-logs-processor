package org.shersfy.mr;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogsProcessorMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

    private Logger logger = LoggerFactory.getLogger(getClass());
    @Override
    protected void map(LongWritable key, Text value,
                       Mapper<LongWritable, Text, Text, LongWritable>.Context context)
                           throws IOException, InterruptedException {

        if(value==null) {
            return;
        }


        int cnt = StringUtils.countMatches(value.toString(), "service:jmx:rmi:///jndi/rmi://zknode3:-1/jmxrmi");
        context.write(new Text("cnt"), new LongWritable(cnt));

        logger.info("offsite: {}, countMatches={}", key, cnt);
    }

}
