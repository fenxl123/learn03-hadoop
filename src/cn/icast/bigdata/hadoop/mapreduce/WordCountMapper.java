package cn.icast.bigdata.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException 
{
	String  line=value.toString();
	String [] values=line.split(" ");
	for(String word:values){
		context.write(new Text(word), new IntWritable(1));
	}

};

}
