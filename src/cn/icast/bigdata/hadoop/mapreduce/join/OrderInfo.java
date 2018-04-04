package cn.icast.bigdata.hadoop.mapreduce.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class OrderInfo  implements Writable{
	private int id;
	private String date;
	private String pid;
	private int amount;
	private String name;
	private String category_id;
	private int price;
	//0表示订单信息，1表示商品信息
	private String flag;
	
	public void set (int id, String date, String pid, int amount, String name,
			String category_id, int price, String flag) {
		this.id = id;
		this.date = date;
		this.pid = pid;
		this.amount = amount;
		this.name = name;
		this.category_id = category_id;
		this.price = price;
		this.flag = flag;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public String getPid() {
		return pid;
	}
	public void setPid(String pid) {
		this.pid = pid;
	}
	public int getAmount() {
		return amount;
	}
	public void setAmount(int amount) {
		this.amount = amount;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getCategory_id() {
		return category_id;
	}
	public void setCategory_id(String category_id) {
		this.category_id = category_id;
	}
	public int getPrice() {
		return price;
	}
	public void setPrice(int price) {
		this.price = price;
	}
	public String getFlag() {
		return flag;
	}
	public void setFlag(String flag) {
		this.flag = flag;
	}
	@Override
	public String toString() {
		return "OrderInfo [id=" + id + ", date=" + date + ", pid=" + pid
				+ ", amount=" + amount + ", name=" + name + ", category_id="
				+ category_id + ", price=" + price + ", flag=" + flag + "]";
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
		out.writeInt(id);
		out.writeUTF(date);
		out.writeUTF(pid);
		out.writeInt(amount);
		out.writeUTF(name);
		out.writeUTF(category_id);
		out.writeInt(price);
		out.writeUTF(flag);
		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.id=in.readInt();
		this.date=in.readUTF();
		this.pid=in.readUTF();
		this.amount=in.readInt();
		this.name=in.readUTF();
		this.category_id=in.readUTF();
		this.price=in.readInt();
		this.flag=in.readUTF();
				
				
		
	}
	
}
