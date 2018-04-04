package cn.icast.bigdata.hadoop.mapreduce.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.icast.bigdata.hadoop.mapreduce.WodCountReduce;
import cn.icast.bigdata.hadoop.mapreduce.WordCountDriver;
import cn.icast.bigdata.hadoop.mapreduce.WordCountMapper;

public class Indexmain {
  static class IndexMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	  Text k=new Text();
	  IntWritable v=new IntWritable(1);
	  @Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		    FileSplit inputspilt=(FileSplit) context.getInputSplit();
		    String filename= inputspilt.getPath().getName();
		    String line= value.toString();
		    String[] file= line.split(" ");
		    for(String word:file){
			     k.set(word+"--"+filename);
				 context.write(k, v);
		    	
		  }
		   
	}
  }
  static class IndexReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
	  @Override
	protected void reduce(Text key, Iterable<IntWritable> value,
			Context context)
			throws IOException, InterruptedException {
		  int count=0;
		  for(IntWritable va:value){
			  count+=va.get();
			  
		  }
		context.write(key, new IntWritable(count));
	}
	  
  }
  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		   Job job=Job.getInstance(conf);
		   //ָ��jar�����ڵı���·��
		   job.setJarByClass(Indexmain.class);
		   job.setMapperClass(IndexMapper.class);
		   job.setReducerClass(IndexReduce.class);
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
		    FileInputFormat.setInputPaths(job, new Path("D:/��Ŀ��������/input/index"));
		    FileOutputFormat.setOutputPath(job, new Path("D:/output"));
		    //��job�������õĲ������Լ�job���õ�java������jar�����ύ��yarnȥ����
		   Boolean red= job.waitForCompletion(true);
		   System.exit(red?0:1);
		  
}
}
