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
	   //ָ��jar�����ڵı���·��
	   job.setJarByClass(WordCountDriver.class);
	   job.setMapperClass(WordCountMapper.class);
	   job.setReducerClass(WodCountReduce.class);
	   //ָ���Զ�������ݷ�����
	   //job.setPartitionerClass(WordCountPrititioner.class);
	   //�����ʱ��Ubuntu�ϲ�
	    //job.setCombinerClass(WodCountReduce.class);
	   //ָ��reduce������
	   //job.setNumReduceTasks(3);
	   job.setMapOutputKeyClass(Text.class);
	   job.setMapOutputValueClass(IntWritable.class);
	   job.setOutputKeyClass(Text.class);
	   job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    //��job�������õĲ������Լ�job���õ�java������jar�����ύ��yarnȥ����
	   Boolean red= job.waitForCompletion(true);
	   System.exit(red?0:1);
	  
	}
}
 