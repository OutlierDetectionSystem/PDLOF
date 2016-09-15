package metricspace;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

@SuppressWarnings("rawtypes")
public class MetricObject implements Comparable {
	public int pid;
	public int partition_id;
	public char type;
	public float distToPivot;
	public Object obj;
	public String whoseSupport="";
	public String KNN = "";
	public Map<Integer,Float> knnInDetail = new HashMap<Integer,Float>();
	public float kdist = 0;
	public float lrd = 0;
	public boolean canCalculateLof = false;
	public MetricObject() {
	}

	public MetricObject(int pid, float dist, char type, int partition_id, String whoseSupport, Object obj) {
		this.pid = pid;
		this.distToPivot = dist;
		this.type = type;
		this.partition_id = partition_id;
		this.obj = obj;
		this.whoseSupport = whoseSupport;
	}
	public MetricObject(int pid, float dist, char type, int partition_id, String whoseSupport, Object obj, float kdistance, String KNN){
		this(pid,dist,type,partition_id,whoseSupport,obj);
		this.KNN = KNN;
		this.kdist = kdistance;
	}
	public String toString() {
		StringBuilder sb = new StringBuilder(); 
		Record r = (Record) obj;
		sb.append(",pid: " + pid);
		sb.append(",distToPivot:" + distToPivot);
		sb.append(",type: "+type);
		sb.append(",in Partition: "+partition_id);
		sb.append(", whoseSupport: "+ whoseSupport);
		sb.append("," + r.getRId());
		float[] value = r.getValue();

		for (float v : value) {
			sb.append("," + v);
		}
		return sb.toString();
	}
	
	/**
	 * sort by the descending order
	 */
	@Override
	public int compareTo(Object o) {
		MetricObject other = (MetricObject) o;
		if (other.distToPivot > this.distToPivot)
			return 1;
		else if (other.distToPivot < this.distToPivot)
			return -1;
		else
			return 0;
	}

	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		
	}
	
}
