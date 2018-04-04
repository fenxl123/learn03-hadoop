package cn.icast.bigdata.hadoop.mapreduce.join;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FilterOutputFormat;

import cn.icast.bigdata.hadoop.mapreduce.WodCountReduce;
import cn.icast.bigdata.hadoop.mapreduce.WordCountDriver;
import cn.icast.bigdata.hadoop.mapreduce.WordCountMapper;

public class Rjoin {
  static class JoinMapper extends Mapper< LongWritable,Text,Text,OrderInfo>{
	  OrderInfo infobean=new OrderInfo();
	  Text k=new Text();
	  
	 @Override
	protected void map(LongWritable key, Text value,
			Context context)throws IOException, InterruptedException {
		 String pid="";
		String line= value.toString();
		String[] values=line.split(",");
		 FileSplit inputspilt=(FileSplit) context.getInputSplit();
		 String name= inputspilt.getPath().getName();
		 if(name.startsWith("order")){
			 pid=values[2];
			 infobean.set(Integer.parseInt(values[0]), values[1], pid, Integer.parseInt(values[3]), "", "", 0, "0");
		 }else{
			 pid=values[0];
			 infobean.set(0, "", pid, 0, values[1], values[2], Integer.parseInt(values[3]), "1");
		 }
		k.set(pid);
		context.write(k, infobean);
	}
  }
  static class JoinReduce extends Reducer<Text,OrderInfo,OrderInfo,NullWritable>{
	  OrderInfo Info=new OrderInfo();
	  List<OrderInfo> orderbean=new ArrayList<OrderInfo>();
	  
	protected void reduce(Text key, Iterable<OrderInfo> OrderInfo,
			Context context)
			throws IOException, InterruptedException {
		  for(OrderInfo bean: OrderInfo){
			  if(bean.getFlag().equals("1")){
				try {
					BeanUtils.copyProperties(Info,bean);
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}  
			  }
			  else{
				  orderbean.add(bean) ;
			  }
		  }
		  for(OrderInfo order:orderbean){
			  order.setName(Info.getName());
			  order.setCategory_id(Info.getCategory_id());
			  order.setPrice(Info.getPrice());
			  context.write(order, NullWritable.get()); 
		  }
	      
	}
  }
  public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		   Job job=Job.getInstance(conf);
		   //指定jar包所在的本地路径
		   job.setJarByClass(Rjoin.class);
		   job.setMapperClass(JoinMapper.class);
		   job.setReducerClass(JoinReduce.class);
		   //指定自定义的数据分区器
		   //job.setPartitionerClass(WordCountPrititioner.class);
		   //溢出的时候Ubuntu合并
		    //job.setCombinerClass(WodCountReduce.class);
		   //指定reduce的数量
		   //job.setNumReduceTasks(3);
		   job.setMapOutputKeyClass(Text.class);
		   job.setMapOutputValueClass(OrderInfo.class);
		   job.setOutputKeyClass(OrderInfo.class);
		   job.setOutputValueClass(NullWritable.class);
		   FileInputFormat.setInputPaths(job, new Path(args[0]));
		   FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    //将job中所配置的参数，以及job所用的java类所在jar包，提交给yarn去运行
		   Boolean red= job.waitForCompletion(true);
		   System.exit(red?0:1);
}
}
