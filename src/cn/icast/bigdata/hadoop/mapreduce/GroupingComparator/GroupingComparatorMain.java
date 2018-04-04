package cn.icast.bigdata.hadoop.mapreduce.GroupingComparator;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.icast.bigdata.hadoop.mapreduce.WodCountReduce;
import cn.icast.bigdata.hadoop.mapreduce.WordCountDriver;
import cn.icast.bigdata.hadoop.mapreduce.WordCountMapper;

public class GroupingComparatorMain {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		   Configuration conf=new Configuration();
		   Job job=Job.getInstance(conf);
		   //指定jar包所在的本地路径
		   job.setJarByClass(GroupingComparatorMain.class);
		   job.setMapperClass(GroupingComparatorMapper.class);
		   job.setReducerClass(GroupingComparatorReduce.class);
		   job.setPartitionerClass(MyPartitioner.class);
		   job.setGroupingComparatorClass(MyGroupingComparator.class);
		   //指定自定义的数据分区器
		   //job.setPartitionerClass(WordCountPrititioner.class);
		   //溢出的时候Ubuntu合并
		    //job.setCombinerClass(WodCountReduce.class);
		   //指定reduce的数量
		   //job.setNumReduceTasks(3);
		   job.setMapOutputKeyClass(Order.class);
		   job.setMapOutputValueClass(NullWritable.class);
		   job.setOutputKeyClass(Text.class);
		   job.setOutputValueClass(DoubleWritable.class);
		   FileInputFormat.setInputPaths(job, new Path("D:/项目资料人寿/input/GroupingComparator"));
		   FileOutputFormat.setOutputPath(job, new Path("D:/项目资料人寿/input/output"));
		    //将job中所配置的参数，以及job所用的java类所在jar包，提交给yarn去运行job
		   Boolean red= job.waitForCompletion(true);
		   System.exit(red?0:1);
	}
}
