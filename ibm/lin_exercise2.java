import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class lin_exercise2 extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

	private DoubleWritable result = new DoubleWritable();
	private Text index = new Text();

	public void configure(JobConf job) {
	}

	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
	    String line = value.toString();
		
		// break different fields in the line by ,
	    String[] columns = line.split(",");
	    
	   
	    // process last column equals false
	    if (columns[columns.length-1].equals("false"))

	    {
	    	
	    	// columns 30,31,32,33 (location 29,30,31,32) as index aggregate
		 	String indexS = columns[29].substring(0, 1) +
		 			  	    columns[30].substring(0, 1) +
		 			  	    columns[31].substring(0, 1) +
		 			  	    columns[32].substring(0, 1) ;
		 	
		 	// column 4 value to average
		 	Double resultD = Double.parseDouble(columns[3]);
	    	
		 	index.set(indexS);
			result.set(resultD);
		
			output.collect(index, result);
	    	
	    }
		
		
		
		
		
	}
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
	    
	    Double sum = 0.0;
	    int count = 0;
	    while (values.hasNext()) {
	    	count++;
			sum += values.next().get();
	    }
	    output.collect(key, new DoubleWritable(sum/count));
	}
    }

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), lin_exercise2.class);
	conf.setJobName("averageibm");

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(DoubleWritable.class);

	conf.setMapperClass(Map.class);
	// conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new lin_exercise2(), args);
	System.exit(res);
    }
}