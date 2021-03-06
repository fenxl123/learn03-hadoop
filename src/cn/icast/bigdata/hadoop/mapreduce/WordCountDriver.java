package cn.icast.bigdata.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class WordCountDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
	   Job job=Job.getInstance(conf);
	   //指定jar包所在的本地路径
	   job.setJarByClass(WordCountDriver.class);
	   job.setMapperClass(WordCountMapper.class);
	   job.setReducerClass(WodCountReduce.class);
	   //指定自定义的数据分区器
	   //job.setPartitionerClass(WordCountPrititioner.class);
	   //溢出的时候Ubuntu合并
	    //job.setCombinerClass(WodCountReduce.class);
	   //指定reduce的数量
	   //job.setNumReduceTasks(3);
	   job.setMapOutputKeyClass(Text.class);
	   job.setMapOutputValueClass(IntWritable.class);
	   job.setOutputKeyClass(Text.class);
	   job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    //将job中所配置的参数，以及job所用的java类所在jar包，提交给yarn去运行
	   Boolean red= job.waitForCompletion(true);
	   System.exit(red?0:1);
	  
	}
}
 