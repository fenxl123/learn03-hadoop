package cn.icast.bigdata.hadoop.mapreduce.friendship;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ShipMapper extends  Mapper <LongWritable,Text,Text,Text>{
	String user="";
	String  friendship="";
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		 String line=value.toString();
		 String[] values= line.split(":");
		   user=values[0];
		   friendship=values[1];
		   String[] ship=friendship.split(",");
		   for(String friend :ship)
			   context.write(new Text(friend), new Text(user));
		 
	}

}
