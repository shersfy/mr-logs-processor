package org.shersfy.mr;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

public class LogsProcessor {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        
        System.setProperty("HADOOP_USER_NAME", "hadoop");
//        conf.set("fs.defaultFS", "hdfs://master:9000");
        JobConf jobconf = new JobConf(true);
        jobconf.setJar(LogsProcessor.class.getClassLoader().getResource("mr-logs-processor.jar").getPath());
        jobconf.set("mapreduce.app-submission.cross-platform", "true");
        
        // 1. 设置map key value类型
        jobconf.setMapOutputKeyClass(Text.class);
        jobconf.setMapOutputValueClass(LongWritable.class);
        
        // 2. 设置reduce key value类型
        jobconf.setOutputKeyClass(Text.class);
        jobconf.setOutputValueClass(LongWritable.class);
        
        // 3. 设置map输入路径和reduce输出路径
        FileInputFormat.setInputPaths(jobconf, new Path("/data/input"));
        FileOutputFormat.setOutputPath(jobconf, new Path("/data/output"));
        
        // 4. 设置mapper类和reduce类
        Job job = Job.getInstance(jobconf, "Job_LogsProcessor");
        job.setMapperClass(LogsProcessorMapper.class);
        job.setReducerClass(LogsProcessorReduce.class);
        
        // 5.提交job
        job.submit();
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }

}
