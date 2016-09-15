package preprocess.pivotselection.kmean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataPro implements WritableComparable<DataPro> {
	// 中心向量
	private Text center;
	// 中心向量对应分组所拥有的个数
	private IntWritable count;
	private static Logger log = LoggerFactory.getLogger(DataPro.class);
	public DataPro(){
		set(new Text(),new IntWritable());
	}
	public  void set(Text textWritable, IntWritable intWritable) {
		// TODO Auto-generated method stub
		this.center=textWritable;
		this.count=intWritable;
	}
	/**
	 * 获得中心向量
	 * @return
	 */
	public Text getCenter(){
		return center;
	}
	/**
	 * 获得中心向量分组所拥有的个数
	 * @return
	 */
	public IntWritable getCount(){
		return count;
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		//log.info("in read() ----------------------------center:"+center+"--count:"+count);
		center.readFields(arg0);
		count.readFields(arg0);
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		//log.info("in write()-------------center:"+center+"-count:"+count);
		center.write(arg0);count.write(arg0);}
	@Override
	public int compareTo(DataPro o) {
			
		int cmp=center.compareTo(o.center);
		if(cmp!=0){
			return cmp;
		}
		return count.compareTo(o.count);
	}
}