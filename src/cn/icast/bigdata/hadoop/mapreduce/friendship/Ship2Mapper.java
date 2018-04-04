package cn.icast.bigdata.hadoop.mapreduce.friendship;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Ship2Mapper extends  Mapper <LongWritable,Text,Text,Text> {
	String friendkey="";
	String friendvalue="";
	
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		String line= value.toString();
	    String[] values=line.split(" ");
	    friendkey=values[0];
	    friendvalue=values[1];
	    context.write(new Text(friendkey), new Text(friendvalue));
		 
	}
}
