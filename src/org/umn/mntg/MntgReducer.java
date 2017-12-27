package org.umn.mntg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
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
	static ArrayList<MntgPoint> list = new ArrayList<MntgPoint>();
	static HashSet<MntgPoint> uniqueValues = new HashSet<MntgPoint>();
	static ArrayList<MntgPoint> uniqueList = new ArrayList<MntgPoint>();
	
	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		// 4953-0,newpoint,2017-08-03 22:45,128.4927315,34.8878539
		 
		
		while (values.hasNext()) {
			String[] temp = values.next().toString().split(",");
			MntgPoint point = new MntgPoint(temp);
			list.add(point);
		}
		
		uniqueValues = new HashSet<>(list);
		uniqueList = new ArrayList(uniqueValues);
		Collections.sort(uniqueList);
		for(MntgPoint p : uniqueList){
			if(reduceValue.toString().contains(Double.toString(p.x)) && 
					reduceValue.toString().contains(Double.toString(p.y)) ){
				continue;
			}else
			{
				reduceValue.append(","+p.date+","+p.x+","+p.y);
			}
					
			
		}
		output.collect(key, new Text(reduceValue.toString()));
		
	}
	
	

}
