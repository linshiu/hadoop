import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class lin_exercise1 extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private IntWritable temp = new IntWritable();
    private IntWritable year = new IntWritable();
	

	public void configure(JobConf job) {
	}

	public void map(LongWritable key, Text value, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
	    String line = value.toString();

	    String year_s = line.substring(15, 19);
		String temp_s = line.substring(87, 92);

		// getting errors with "+0001"
		if (temp_s.substring(0,1).equals("+")){
			temp_s = temp_s.substring(1,5); // remove +
		}

		int year_int = Integer.parseInt(year_s);
		int temp_int = Integer.parseInt(temp_s);

		String quality = line.substring(92,93);
		String goodQuality = "01459";
		
		// only process temperature reading is accurate and satisfactory
		// and not missing (different than 9999 or -9999)
		if (goodQuality.contains(quality) & 
				temp_int!=9999 & temp_int!=-9999) 
		{
			
	

			year.set(year_int);
			temp.set(temp_int);

			output.collect(year, temp);
			
		} // end if


	}
    }


    public static class Reduce extends MapReduceBase implements Reducer< IntWritable, IntWritable, IntWritable, IntWritable> {
	public void reduce( IntWritable key, Iterator<IntWritable> values, OutputCollector< IntWritable, IntWritable> output, Reporter reporter) throws IOException {

	    int max = -9999;
	   
	    // iterating over values of the same key
	    // for every key, reducer is going to be executed 

	    while (values.hasNext()) {
			int current = values.next().get();
			if (current>max){max=current;}
	    }

	    output.collect(key, new IntWritable(max));
	}
    }

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), lin_exercise1.class);
	conf.setJobName("maxemperature");

	conf.setOutputKeyClass(IntWritable.class);
	conf.setOutputValueClass(IntWritable.class);

	conf.setMapperClass(Map.class);
	conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new lin_exercise1(), args);
	System.exit(res);
    }
}