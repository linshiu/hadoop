// default import not working, so import more specific

// import java io
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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

// import disrtriubted cahe
import org.apache.hadoop.filecache.DistributedCache;
//import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Scanner;

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

import java.io.*;
import java.util.*;
import java.util.ArrayList;

import java.io.IOException;
import java.lang.InterruptedException;
import java.util.StringTokenizer;
import java.net.URI; 

import org.apache.hadoop.io.FloatWritable; 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileSplit; 
import org.apache.hadoop.fs.FSDataInputStream; 
import org.apache.hadoop.mapreduce.RecordReader; 
import java.io.File;
import java.util.Scanner; 
import org.apache.hadoop.io.IOUtils; 

import java.lang.Math; // for square and square root

public class lin_rehman_exercise {

	//********* Class to pass a pair (count, array list with sums for each variable) ************//
	public static class Pair implements Writable {
	    
	    private int count;
	    private ArrayList<Double> sums;

	    public Pair() {
	    	int count = 0;
			ArrayList<Double> sums= new ArrayList<Double>();

	    }

	    public Pair(int count, ArrayList<Double> sums) {
	       
	        this.count = count;
			this.sums = new ArrayList<Double>(sums);
	    }

	    public int getCount()
		{
        	return count;
   		}

	    public  ArrayList<Double> getSums() {
	        return sums;
	    }

	    public void setCount(int count) {
        	this.count= count;
    	}

	    public void setSums(ArrayList<Double> sums) {
	        this.sums = new ArrayList<Double>(sums);
	    }

	    //serialize
	    @Override
	    public void write(DataOutput out) throws IOException {
	    	out.writeInt(count);

	        int length = 0;
	        if(sums != null) {
	            length = sums.size();
	        }

	        out.writeInt(length);

	        for(int i = 0; i < length; i++) {
	            out.writeDouble(sums.get(i));
	        }
	    }

	    //deserialize
	    @Override
	    public void readFields(DataInput in) throws IOException {
	        
	        count = in.readInt();

	        int length = in.readInt();

	        sums = new ArrayList<Double>();

	        for(int i = 0; i < length; i++) {
	            sums.add(in.readDouble());
	        }
	    }
	}

	//********* Functions to manipulate arraylists ************************************************//

	/**
	 * convertToDouble
	 * @param arrayList string
	 * @return arrayList double
	 */
	public static ArrayList <Double> convertToDouble(ArrayList <String >arrayS){

   		// convert array to double
   		ArrayList<Double> arrayD = new ArrayList<Double>();
   		
   		for (String item:arrayS){
   			arrayD.add(Double.parseDouble(item));
   		}
   		
   		return arrayD;
		
	} // end convertToDouble

	/**
	 * convertToString
	 * @param arrayList double
	 * @return string
	 */
	
	public static String convertToString(ArrayList <Double > array){
   		
   		String stringList = "";
		for (double d : array)
		{
			String s = String.valueOf(d);
		    stringList += s + "\t";
		}
		
		return stringList;
		
	} // end convertToString

	/**
	 * sumArrays
	 * @param two arrays of double
	 * @return array with sum element by element
	 */
	public static ArrayList <Double> sumArrays(ArrayList <Double >array1, ArrayList <Double > array2){
	
		ArrayList <Double > sum = new ArrayList <Double >();
		for (int i=0; i< array1.size(); i++){
			
			sum.add(array1.get(i)+array2.get(i));		
		}
	
		return sum;
	} // end sumArrays

	/**
	 * divideArray
	 * @param integer with i, arrays with doubles
	 * @return array with elements divided by i
	 */
	public static ArrayList <Double> divideArray(int i, ArrayList <Double >array){
		
		ArrayList <Double > result = new ArrayList <Double >();
		for (Double value: array){
			
			result.add(value/i);		
		}
		
		return result;
	} // end sumArrays


	/**
	 * findDistance
	 * @param two arrays of double
	 * @return a double representing the euclidean distance
	 */
	public static double findDistance(ArrayList <Double >array1, ArrayList <Double > array2){
		
		double ss = 0;
		for (int i=0; i< array1.size(); i++){
			double d = array1.get(i)-array2.get(i);
			
			ss += d*d;
		}
		
		return Math.sqrt(ss);
	} // end findDistance


	///********* Mapper *************************************************************************//
    public static class MyMapper extends Mapper<LongWritable, Text, Text,Pair> {

	    private Pair pair= new Pair();
	    private Text cluster = new Text();

	    private HashMap<String, ArrayList <Double >> centroids = new HashMap<String, ArrayList<Double>>();
	
	    private File centroidsFile ; 
	    private Scanner in ; 

	    //Scanner column  = new Scanner(line);
		//column.useDelimiter(",");
		//String val = column.next().trim() ;
		//resultD = Double.parseDouble(val)

	    protected void setup(Context context) throws IOException, InterruptedException {
	       centroidsFile = new File("centroids");
	       Scanner in = new Scanner(centroidsFile); 
	  
	       // loop all lines of cnetroidx file
	       while(in.hasNextLine()) { 
	       		String line = in.nextLine(); 
	       		
       			ArrayList<String> list = new ArrayList<String>(Arrays.asList(line.split("\\s+")));

	       		// extract cluster ID
	       		String clusterID = list.get(0);
	       		list.remove(0);

	       		// convert array to double
	  
				ArrayList<Double> listD = convertToDouble(list);
	       		centroids.put(clusterID, listD);	
	       			
	       		
			} // end while

			in.close();

	    } // end setup


		// map function
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		    String line = value.toString();

		    if (!line.isEmpty()){

		    	// break different fields in the line by tab
				// or one or more white space "\\s+" since 2gram the two words are not separated by tab
		       	ArrayList<String> recordS = new ArrayList<String>(Arrays.asList(line.split("\\s+")));
		       	ArrayList<Double> record = convertToDouble(recordS);

				double minD = 0;
				String minID = "";
				int iter = 0;
			    for (String c : centroids.keySet()){

			    	double dist = findDistance(record,centroids.get(c));

			    	if (iter == 0) {
			    		minID = c;
			    		minD = dist;
			    	}

			    	if (dist<minD){
			    		minID = c;
			    		minD = dist;
			    	}
					
					iter ++;
			    } // end for loop

			    cluster.set(minID);
			    pair.setCount(1);
			    pair.setSums(record);
			    context.write(cluster,pair);

		    } // end if 
		} // end map function
    } // end map class

    ///********* Combiner *************************************************************************//

    public static class MyCombiner extends Reducer <Text, Pair, Text, Pair> {

    	private Pair pair= new Pair();

		public void reduce( Text key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
			
			ArrayList<Double> sums = new ArrayList<Double>() ;
			int count = 0;
			int iter = 0;

			for (Pair value:values) 
			{	
				count += value.getCount();

				if (iter == 0){
 					sums = new ArrayList<Double>(value.getSums());
				}

				else{
					sums = sumArrays(sums,value.getSums());
				}

				iter++;
				
			} // end for

		    pair.setCount(count);
		    pair.setSums(sums);
		    context.write(key,pair);

		} // end combine
	} // end combiner
	
  
	///********* Reducer *************************************************************************//

	
    public static class MyReducer extends Reducer< Text, Pair, Text, Text> {

		public void reduce( Text key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {

			//ArrayList<Double> sum = new ArrayList<Double>(Collections.nCopies(10, 0.0));
			ArrayList<Double> sums = new ArrayList<Double>() ;
			int count = 0;
			int iter = 0;

			for (Pair value:values) 
			{	
				count += value.getCount();

				if (iter == 0){	
 					sums = new ArrayList<Double>(value.getSums());
				}

				else{
					sums = sumArrays(sums,value.getSums());
				}

				iter++;
				
			} // end for

			ArrayList<Double> means = divideArray(count,sums) ;
			context.write(key, new Text(convertToString(means)));

		} // end reduce function 
    } // end reduce class

   

    ///********* Main *************************************************************************//

	public static void main(String[] args) throws Exception {
		// Sources:
	    // Mutiple inputs: http://www.lichun.cc/blog/2012/05/hadoop-multipleinputs-usage/
	    // Custom classs: http://www.cs.bgu.ac.il/~dsp112/Forum?action=show-thread&id=5a15ede6df2520f2b68db15f2ce752fa

		Configuration conf = new Configuration();

		Job job = new Job(conf);
		job.setJarByClass(lin_rehman_exercise.class);
		job.setJobName("avg books");

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setCombinerClass(MyCombiner.class);

	    //output format for mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Pair.class);
		//job.setMapOutputValueClass(DoubleWritable.class);

	    //output format for reducer
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

	     //use MultipleOutputs and specify different Record class and Input formats
		//MultipleInputs.addInputPath(job, firstPath, TextInputFormat.class, MyMapper1.class);
		//MultipleInputs.addInputPath(job, sencondPath, TextInputFormat.class, MyMapper2.class);

		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		String distributedCachePath = new Path(args[2]).toString() + "#" + "centroids";

		//FileInputFormat.setInputPaths(job, inputPath);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		//Distributed cache

		//URI partitionUri = new URI("/user/huser76/DC/centroids.txt#centroids");
		URI partitionUri = new URI(distributedCachePath);
    	DistributedCache.addCacheFile(partitionUri,job.getConfiguration());
    	DistributedCache.createSymlink(job.getConfiguration());

		//job.setNumReduceTasks(0);
		//conf.setPartitionerClass(MusicPartitioner.class)

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}		
}