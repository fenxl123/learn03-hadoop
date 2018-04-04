package cn.icast.bigdata.hadoop.mapreduce.GroupingComparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;


public class MyPartitioner extends Partitioner<Order, NullWritable> {

	@Override
	public int getPartition(Order order, NullWritable value, int numPartitions) {
		
		return(order.getItemid().hashCode()&Integer.MAX_VALUE) % numPartitions ;
	}



}
