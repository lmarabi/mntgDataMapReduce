package org.umn.mntg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MntgReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {
	
	StringBuilder reduceValue = new StringBuilder();
	static ArrayList<mntgPoint> list = new ArrayList<MntgReducer.mntgPoint>();
	
	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		// 4953-0,newpoint,2017-08-03 22:45,128.4927315,34.8878539
		 
		
		while (values.hasNext()) {
			String[] temp = values.next().toString().split(",");
			mntgPoint point = new mntgPoint(temp);
			list.add(point);
		}
		
		
		Collections.sort(list);
		for(mntgPoint p : list){
			reduceValue.append(","+p.date+","+p.x+","+p.y);
		}
		output.collect(key, new Text(reduceValue.toString()));
	}
	
	
	private class mntgPoint implements Comparable<mntgPoint>{
		String type;
		String date;
		double x;
		double y;
		
		public mntgPoint(String[] valueString) {
			this.type = valueString[0];
			this.date = valueString[1]; 
			this.x = Double.parseDouble(valueString[2]);
			this.y = Double.parseDouble(valueString[3]);
		}
		
		@Override
		public int compareTo(mntgPoint point) {
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

}
