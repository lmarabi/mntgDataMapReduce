package org.umn.mntg;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;


public class MntgDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		int result = 0;
		Path input = args.length > 0 ? new Path(args[0]) : new Path(
				System.getProperty("user.dir") + "/data/*/*/");
		Path output1 = args.length > 0 ? new Path(args[1]) : new Path(
				System.getProperty("user.dir") + "/hdfs/hdfsoutput");

		// First Map-Reduce Job
		JobConf conf = new JobConf(getConf(), MntgDriver.class);
		//DistributedCache.addCacheFile(new Path(mbrFile).toUri(), conf);
		FileSystem outfs = output1.getFileSystem(conf);
		outfs.delete(output1, true);

		conf.setJobName("mntg Driver");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(MntgMapper.class);
		conf.setCombinerClass(MntgReducer.class);
		conf.setReducerClass(MntgReducer.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, input);
		FileOutputFormat.setOutputPath(conf, output1);

	    JobClient.runJob(conf).waitForCompletion();
		System.out.println("Job1 finish");
		return 0;
	}

}
