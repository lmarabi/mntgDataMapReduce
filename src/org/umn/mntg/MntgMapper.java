package org.umn.mntg;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class MntgMapper extends MapReduceBase implements
		Mapper<Object, Text, Text, Text> {

	private Text mapKey = new Text();
	private Text mapValue = new Text();

	@Override
	public void map(Object key, Text value, OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		// 4953-0,newpoint,2017-08-03 22:45,128.4927315,34.8878539
		String csv = value.toString();
		String[] line = csv.split(",");
		if (line.length == 5) {
			mapKey.set(line[0]);
			mapValue.set(csv.substring(line[0].length()+1, csv.length()));
			output.collect(mapKey, mapValue);
		}

	}

}