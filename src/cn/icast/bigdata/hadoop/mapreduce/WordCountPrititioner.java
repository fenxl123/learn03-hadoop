package cn.icast.bigdata.hadoop.mapreduce;

import java.util.HashMap;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class WordCountPrititioner implements Partitioner {
	private  static HashMap<String,Integer> partitioner=new HashMap<String,Integer>();
	static{
		partitioner.put("158", 0);
		partitioner.put("138", 1);
		partitioner.put("188", 2);
	}

	@Override
	public void configure(JobConf job) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getPartition(Object key, Object value, int numPartitions) {
	String guishu=key.toString().substring(0, 3);
    Integer integer=partitioner.get(guishu);
    return integer==null?3:integer;
	} 
	
}
