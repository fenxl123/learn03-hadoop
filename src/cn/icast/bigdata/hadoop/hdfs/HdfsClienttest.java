package cn.icast.bigdata.hadoop.hdfs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import javax.servlet.Servlet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

public class HdfsClienttest {
	FileSystem fs=null;
	private static int x=100;
	@Before
	public void init() throws IOException{
		Configuration conf= new Configuration();
		fs=FileSystem.get(conf);
	}
	@Test
	public void testWrite() throws IllegalArgumentException, IOException{
		fs.copyFromLocalFile(new Path("D:/learn/�����ݽ̳�/RpcServer.java"), new Path("/"));
		fs.close();
	}
	
	@Test
	public void demo(){
		try{
			int a = 5/0;  //�쳣�׳���
		}
		catch(NullPointerException e) {
	            //System.out.println("�����쳣");
				throw new ArithmeticException();
		}
		System.out.println("Ϊʲô���������ǹ���!!!"); 
	}
	@Test
	public void haha(){
		try{
		  demo();
		}
		catch (Exception e){
			System.out.println("�����쳣");
		}
	}
	
}
