package org.umn.mntg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

public class MapReduceMain {

	public static void main(String[] args) throws Exception {
		int result = 0;

		Path input = args.length > 0 ? new Path(args[0]) : new Path(
				"/export/scratch/mntgData/test/oneobj");
		Path output = args.length > 0 ? new Path(args[1]) : new Path(
				"/export/scratch/mntgData/result/");
		
		args = new String[2];
		args[0] = input.toString();
		args[1] = output.toString();

		result = ToolRunner.run(new Configuration(), new MntgDriver(),
					args);
		
		System.out.println("Job1 dist finish with value: " + result);
			
	}

}
