package org.umn.mntg;


public class MntgPoint implements Comparable<MntgPoint>{
	String type;
	String date;
	double x;
	double y;
	
	public MntgPoint(String[] valueString) {
		this.type = valueString[0];
		this.date = valueString[1]; 
		this.x = Double.parseDouble(valueString[2]);
		this.y = Double.parseDouble(valueString[3]);
	}
	
	@Override
	public int compareTo(MntgPoint point) {
		  // Sort by id
		  double difference = this.x - point.x;
			if (difference == 0) {
				difference = this.y - point.y;
			}
			if (difference == 0)
			  return 0;
			return difference > 0 ? 1 : -1;
	}
}