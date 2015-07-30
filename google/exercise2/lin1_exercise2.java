// default import not working, so import more specific

// import java io
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.Math; // for square and square root

// import hadoop io fomrmat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

// import hadoop setup
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

// import hadoop types
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

// import hadoop classes
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/*
import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
*/

/*

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
*/

/*

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

*/

public class lin1_exercise2 {

	// custom class
	public static class Triple implements Writable {
		private long value1;    // count
		private long value2;    // sum
		private long value3; // sum sq
		
		// initialize
		public Triple() {
			long value1 = 0;
			long value2= 0;
			long value3 = 0;
		}
		
		// set values
		public Triple(long value1, long value2, long value3) {
			this.value1 = value1;
			this.value2 = value2;
			this.value3 = value3;
		}

		// getters
		public long getValue1()
		{
        	return value1;
   		}

   		public long getValue2()
		{
        	return value2;
   		}

   		public long getValue3()
		{
        	return value3;
   		}

   		// setters
    	public void setValue1(long value1) {
        	this.value1 = value1;
    	}

    	public void setValue2(long value2) {
        	this.value2 = value2;
    	}

    	public void setValue3(long value3) {
        	this.value3 = value3;
    	}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			value1 = in.readLong( );
			value2 = in.readLong( );
			value3 = in.readLong( );
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(value1);
			out.writeLong(value2);
			out.writeLong(value3);
		}
	}


	// map class for 1gram
    public static class MyMapper1 extends Mapper<LongWritable, Text, Text, Triple> {

	    private static final Text dummy = new Text("");
	    private Triple triple= new Triple();
		
		// map function to process 1gram
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		    String line = value.toString();

			// break different fields in the line by tab
			// or one or more white space "\\s+" since 2gram the two words are not separated by tab
		    String[] columns = line.split("\\s+");
		    //System.out.println(Arrays.toString(columns));
		   
		    // convert to integer
		    // http://stackoverflow.com/questions/8336607/how-to-check-if-the-value-is-integer-in-java
		    
		    // compute only if volumes is an integer
		    try 
		    {	
		    	// assuming want all records, even if year is a string to comment out
		        //Integer.parseInt(columns[1]);

	        	//System.out.println(index + " " + columns[3]);	
	        	long volume = Long.parseLong(columns[3]);
	        	long volume2 = volume*volume;
	        	triple.setValue1(1);
	        	triple.setValue2(volume);
	        	triple.setValue3(volume2);
				context.write(dummy,triple);
	    
		    
		    } // end try
		    		    
		    catch (NumberFormatException e)  { } //do nothing
		     

		} // end map function
    } // end map class

    // map class for 2gram
    public static class MyMapper2 extends Mapper<LongWritable, Text, Text, Triple> {

	    private static final Text dummy = new Text("");
	    private Triple triple= new Triple();
		
    	// map function to process 2gram
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		    String line = value.toString();

			// break different fields in the line by tab
			// or one or more white space "\\s+" since 2gram the two words are not separated by tab
		    String[] columns = line.split("\\s+");
		    //System.out.println(Arrays.toString(columns));
		   
		    // convert to integer
		    // http://stackoverflow.com/questions/8336607/how-to-check-if-the-value-is-integer-in-java
		    
		    // compute only if volumes is an integer
		    try 
		    {	
		    	// assuming want all records, even if year is a string to comment out
		        //Integer.parseInt(columns[1]);

	        	//System.out.println(index + " " + columns[3]);	
	        	long volume = Long.parseLong(columns[4]);
	        	long volume2 = volume*volume;
	        	triple.setValue1(1);
	        	triple.setValue2(volume);
	        	triple.setValue3(volume2);
				context.write(dummy,triple);
		      
		    
		    } // end try
		    catch (NumberFormatException e)  { } //do nothing

		} // end map function
    } // end map class

    

    public static class MyCombiner extends Reducer <Text, Triple, Text, Triple> {

    	private Triple triple= new Triple();

		public void reduce( Text key, Iterable<Triple> values, Context context) throws IOException, InterruptedException {
			
			long count = 0;
			long sum = 0;
			long sum2 = 0;

			for (Triple value:values) {

				count += value.value1;
				sum += value.value2;
				sum2 += value.value3;
				
			}

			triple.setValue1(count);
			triple.setValue2(sum);
			triple.setValue3(sum2);
			context.write( key, triple );
		}
	}

	

    public static class MyReducer extends Reducer< Text, Triple, Text, DoubleWritable> {

    	//private DoubleWritable stdev= new DoubleWritable();
		public void reduce( Text key, Iterable<Triple> values, Context context) throws IOException, InterruptedException {

			long count = 0;
			double sum= 0; // double since otherwise divide two integers get an integer
			double sum2 = 0;
			
			for (Triple value:values) 
			{
				count += value.value1;
				sum += value.value2;
				sum2 += value.value3;
				
			}

			double stdev = Math.sqrt(sum2/count - (sum*sum)/(count*count));

			context.write( key, new DoubleWritable(stdev));
			//String stdev = String.valueOf(count) + " " + String.valueOf(sum) + " " + String.valueOf(sum2);
			//context.write( key, new Text(stdev));
		}
    }

	public static void main(String[] args) throws Exception {
		// Sources:
	    // Mutiple inputs: http://www.lichun.cc/blog/2012/05/hadoop-multipleinputs-usage/
	    // Custom classs: http://www.cs.bgu.ac.il/~dsp112/Forum?action=show-thread&id=5a15ede6df2520f2b68db15f2ce752fa

		Path firstPath = new Path(args[0]);
		Path sencondPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);

		Configuration conf = new Configuration();

		Job job = new Job(conf);
		job.setJarByClass(lin1_exercise2.class);
		job.setJobName("avg books");

		//conf.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setCombinerClass(MyCombiner.class);

	    //output format for mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Triple.class);

	    //output format for reducer
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

	     //use MultipleOutputs and specify different Record class and Input formats
		MultipleInputs.addInputPath(job, firstPath, TextInputFormat.class, MyMapper1.class);
		MultipleInputs.addInputPath(job, sencondPath, TextInputFormat.class, MyMapper2.class);
		//FileInputFormat.setInputPaths(conf, new Path(args[0]));

		//conf.setInputFormat(TextInputFormat.class);
		//conf.setOutputFormat(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, outputPath);

		//conf.setNumReduceTasks(5);
		//conf.setPartitionerClass(MusicPartitioner.class

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}		
}