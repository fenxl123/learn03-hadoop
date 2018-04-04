package cn.icast.bigdata.hadoop.mapreduce.GroupingComparator;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class GroupingComparatorMapper extends Mapper<LongWritable,Text,Order,NullWritable> {
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		    String line= value.toString();
		    String[] values= line.split(",");
		    Order order=new Order();
		    order.set(new Text(values[0]), new DoubleWritable(Double.parseDouble(values[2])));
		    context.write(order, NullWritable.get());
	}
	
}
 


