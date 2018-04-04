package cn.icast.bigdata.hadoop.mapreduce.join;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.icast.bigdata.hadoop.mapreduce.join.Rjoin.JoinMapper;
import cn.icast.bigdata.hadoop.mapreduce.join.Rjoin.JoinReduce;

public class MapsideJoin {
	
	static class MapperSide extends Mapper< LongWritable,Text,Text,NullWritable>{
		Map<String,String> map=new HashMap<String,String>();
		Text k=new Text();
		@Override
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException{
		 BufferedReader bf=new  BufferedReader(new InputStreamReader(new FileInputStream("new2.txt")));
		String line;
		while(StringUtils.isNotEmpty(line=bf.readLine())){
			String[] file=line.split(",");
			map.put(file[0], file[1]);
		}
		bf.close();
		}
		@Override
		protected void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
			String orderline= value.toString();
			String[] file=orderline.split(",");
			String name=(String) map.get(file[2]);
			k.set(orderline+","+name);
			context.write(k, NullWritable.get());
			
		}
		
	}
	
 public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
	 Configuration conf=new Configuration();
	   Job job=Job.getInstance(conf);
	   //指定jar包所在的本地路径
	   job.setJarByClass( MapsideJoin.class);
	   job.setMapperClass(MapperSide.class);
	   job.setMapOutputKeyClass(Text.class);
	   job.setMapOutputValueClass(NullWritable.class);
	   FileInputFormat.setInputPaths(job, new Path("D:/项目资料人寿/input"));
	   FileOutputFormat.setOutputPath(job, new Path("D:/output"));
	   job.setNumReduceTasks(0);
	   job.addCacheFile(new URI("file:/D:/learn/大数据教程/hadoop/mapreduce/new2.txt"));
	    //将job中所配置的参数，以及job所用的java类所在jar包，提交给yarn去运行
	   Boolean red= job.waitForCompletion(true);
	   System.exit(red?0:1);
}
}
