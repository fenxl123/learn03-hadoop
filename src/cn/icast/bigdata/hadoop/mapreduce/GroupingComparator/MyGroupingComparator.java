package cn.icast.bigdata.hadoop.mapreduce.GroupingComparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroupingComparator extends WritableComparator {
	protected MyGroupingComparator() {
		super(Order.class, true);
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Order abean = (Order) a;
		Order bbean = (Order) b;
		
		//比较两个bean时，指定只比较bean中的orderid
		return abean.getItemid().compareTo(bbean.getItemid());
		
	}
}
