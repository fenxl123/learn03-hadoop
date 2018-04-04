package cn.icast.bigdata.hadoop.mapreduce.GroupingComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Order  implements WritableComparable<Order>{
	private Text itemid;
	private DoubleWritable amount;
	public Order() {
	}
	public Order(Text itemid, DoubleWritable amount) {
		set(itemid, amount);
	}

	public void set(Text itemid, DoubleWritable amount) {

		this.itemid = itemid;
		this.amount = amount;

	}

	public Text getItemid() {
		return itemid;
	}

	public DoubleWritable getAmount() {
		return amount;
	}


	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(itemid.toString());
		out.writeDouble(amount.get());

		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		String readUTF = in.readUTF();
		double readDouble = in.readDouble();
		
		this.itemid = new Text(readUTF);
		this.amount= new DoubleWritable(readDouble);

	}

	@Override
	public int compareTo(Order o) {
		int temp=this.itemid.compareTo(o.getItemid());
		if(temp==0){
			temp=-this.amount.compareTo(o.getAmount());
		}
		return temp;
		
	}
	

}
