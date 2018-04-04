package cn.icast.bigdata.hadoop.mapreduce.friendship;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ShipReduce  extends Reducer<Text,Text,Text,Text>{
@Override
protected void reduce(Text key, Iterable<Text>value,
		Context context)
		throws IOException, InterruptedException {
	List<String> list=new ArrayList<String>();
	String friendship="";
	for(Text ship:value){
		 list.add(ship.toString());
	}
	for(int i=0;i<list.size()-1;i++){
		String friend=list.get(i);
		for(int j=i+1;j<list.size();j++){
		friendship=friend+"-"+list.get(j);
		context.write(new Text(friendship), key);
		}
		
	}
}
}
