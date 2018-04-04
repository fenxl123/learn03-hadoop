package cn.icast.bigdata.hadoop.mapreduce.GroupingComparator;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GroupingComparatorReduce  extends Reducer<Order,NullWritable,Text,DoubleWritable>{
@Override
protected void reduce(Order order, Iterable<NullWritable> values,
		Context context)
		throws IOException, InterruptedException {
	 context.write(order.getItemid(),order.getAmount());
}
}
