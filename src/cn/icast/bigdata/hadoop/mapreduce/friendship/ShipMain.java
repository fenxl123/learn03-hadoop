package cn.icast.bigdata.hadoop.mapreduce.friendship;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ShipMain {
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		   Job job=Job.getInstance(conf);
		   //ָ��jar�����ڵı���·��
		   job.setJarByClass(ShipMain.class);
		   job.setMapperClass(ShipMapper.class);
		   job.setReducerClass(ShipReduce.class);
		   //ָ���Զ�������ݷ�����
		   //job.setPartitionerClass(WordCountPrititioner.class);
		   //�����ʱ��Ubuntu�ϲ�
		    //job.setCombinerClass(WodCountReduce.class);
		   //ָ��reduce������
		   //job.setNumReduceTasks(3);
		   job.setMapOutputKeyClass(Text.class);
		   job.setMapOutputValueClass(Text.class);
		   job.setOutputKeyClass(Text.class);
		   job.setOutputValueClass(Text.class);
		   FileInputFormat.setInputPaths(job, new Path(args[0]));
		   FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    //��job�������õĲ������Լ�job���õ�java������jar�����ύ��yarnȥ����
		   Boolean red= job.waitForCompletion(true);
		   System.exit(red?0:1);
      }
}

