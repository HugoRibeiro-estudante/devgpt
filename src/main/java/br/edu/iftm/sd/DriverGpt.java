package br.edu.iftm.sd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DriverGpt {
    
        public static void main(String[] args)
            throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "conta palavras");

        job.setJarByClass(DriverGpt.class);
        job.setMapperClass(MapperGpt.class);
        job.setReducerClass(ReducerGpt.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("bases"));
        FileOutputFormat.setOutputPath(job, new Path("saida"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
