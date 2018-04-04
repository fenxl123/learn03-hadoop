package cn.icast.bigdata.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WodCountReduce extends Reducer<Text,IntWritable,Text,IntWritable> {
protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws java.io.IOException ,InterruptedException 

{
	int count=0;
	for(IntWritable value:values){
		count += value.get();
	}
	context.write(key, new IntWritable(count));
};
}
