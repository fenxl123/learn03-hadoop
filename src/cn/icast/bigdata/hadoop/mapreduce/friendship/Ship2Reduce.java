package cn.icast.bigdata.hadoop.mapreduce.friendship;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Ship2Reduce  extends Reducer<Text,Text,Text,Text>{
  @Override
protected void reduce(Text key, Iterable<Text> value,
		
		Context context)
    throws IOException, InterruptedException {
	  Text k=new Text();
	 
	  for(Text text:value){
	    k.set(text.toString());
	  }
	 
	context.write(key,k);
}
}
