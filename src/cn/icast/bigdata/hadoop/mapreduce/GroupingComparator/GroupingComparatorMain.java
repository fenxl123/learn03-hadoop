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
		   //ָ��jar�����ڵı���·��
		   job.setJarByClass(GroupingComparatorMain.class);
		   job.setMapperClass(GroupingComparatorMapper.class);
		   job.setReducerClass(GroupingComparatorReduce.class);
		   job.setPartitionerClass(MyPartitioner.class);
		   job.setGroupingComparatorClass(MyGroupingComparator.class);
		   //ָ���Զ�������ݷ�����
		   //job.setPartitionerClass(WordCountPrititioner.class);
		   //�����ʱ��Ubuntu�ϲ�
		    //job.setCombinerClass(WodCountReduce.class);
		   //ָ��reduce������
		   //job.setNumReduceTasks(3);
		   job.setMapOutputKeyClass(Order.class);
		   job.setMapOutputValueClass(NullWritable.class);
		   job.setOutputKeyClass(Text.class);
		   job.setOutputValueClass(DoubleWritable.class);
		   FileInputFormat.setInputPaths(job, new Path("D:/��Ŀ��������/input/GroupingComparator"));
		   FileOutputFormat.setOutputPath(job, new Path("D:/��Ŀ��������/input/output"));
		    //��job�������õĲ������Լ�job���õ�java������jar�����ύ��yarnȥ����job
		   Boolean red= job.waitForCompletion(true);
		   System.exit(red?0:1);
	}
}
