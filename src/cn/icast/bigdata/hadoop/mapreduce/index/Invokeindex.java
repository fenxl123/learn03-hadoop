package cn.icast.bigdata.hadoop.mapreduce.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable.Comparator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.icast.bigdata.hadoop.mapreduce.index.Indexmain.IndexMapper;
import cn.icast.bigdata.hadoop.mapreduce.index.Indexmain.IndexReduce;

public class Invokeindex {
 static class InvokeindexMapper extends Mapper<LongWritable,Text,Text,Text>{
	 Text k=new Text();
	 Text v=new Text();
	 @Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
	       String line=value.toString();
	       String[] file=line.split("\t");
	       String[] fileindex=file[0].split("--");
	       k.set(fileindex[0]);
	       v.set(fileindex[1]+"--"+file[1]);
	       context.write(k, v);
	}
 }
 static class InvokeindexReduce extends Reducer<Text,Text,Text,Text>{
	@Override
	protected void reduce(Text key, Iterable<Text> value,
			Context context)
			throws IOException, InterruptedException {
		   StringBuffer bf =new StringBuffer();
		   for(Text va:value){
			   bf.append(va+"   ");
		   }
		   context.write(key, new Text(bf.toString()));
	}
 }
 public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		   Job job=Job.getInstance(conf);
		   //指定jar包所在的本地路径
		   job.setJarByClass(Invokeindex.class);
		   job.setMapperClass(InvokeindexMapper.class);
		   job.setReducerClass(InvokeindexReduce.class);
		   //指定自定义的数据分区器
		   //job.setPartitionerClass(WordCountPrititioner.class);
		   //溢出的时候Ubuntu合并
		    //job.setCombinerClass(WodCountReduce.class);
		   //指定reduce的数量
		   //job.setNumReduceTasks(3);
		   job.setMapOutputKeyClass(Text.class);
		   job.setMapOutputValueClass(Text.class);
		   job.setOutputKeyClass(Text.class);
		   job.setOutputValueClass(Text.class);
		   FileInputFormat.setInputPaths(job, new Path("D:/项目资料人寿/input/output"));
		   FileOutputFormat.setOutputPath(job, new Path("D:/output"));
		    //将job中所配置的参数，以及job所用的java类所在jar包，提交给yarn去运行
		   Boolean red= job.waitForCompletion(true);
		   System.exit(red?0:1);
		  
}
}
