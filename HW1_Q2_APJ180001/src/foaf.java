import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import javax.xml.bind.ValidationEvent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.retry.RetryPolicies.MultipleLinearRandomRetry.Pair;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class foaf {

    public static class Map
            extends Mapper<LongWritable, Text, IntWritable, Text>{
    	// Output (Key,Value) => (noOfMutulFriends, userIdPair-mutualFriendList)


    	IntWritable Key= new IntWritable();
    	Text Value = new Text();
    	
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] mydata = value.toString().split("\n");
            
            for (String data : mydata) {
            	String[] line=data.toString().split("\t");
            	if(line.length>1)
            	{
            		String userid_pair= line[0];
                	String[] friendIds= line[1].toString().split(",");
                	int count = friendIds.length;
                	Key.set(count);
                	Value.set(userid_pair+"-"+line[1]);
            	
                	context.write(Key, Value);
                		
            	} 	         	
            }
        }
    }
    
    public static class Compare extends WritableComparator {
        public Compare(){
            super(IntWritable.class, true);
        }
        @Override
        public int compare(WritableComparable wc1, WritableComparable wc2) {
            IntWritable k1 = (IntWritable) wc1;
            IntWritable k2 = (IntWritable) wc2;

            int res=k1.compareTo(k2)*(-1);
            return res;
        }
    }

    public static class Reduce
            extends Reducer<IntWritable,Text,Text,Text> {
    	// Output (Key,Value) => (userIdPair, noOfMutualFriends tab MutualFriendList)

        private IntWritable result = new IntWritable();
        int count = 0;
        Text Key= new Text();
    	Text Value = new Text();
    	
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        for(Text txt : values)
        {
        	if(count == 10)
        	{
        		break;
        	}
        	else {
        		count+=1;
        		String ids[]=txt.toString().split("-");
        		Key.set(ids[0]);
        		Value.set(key.toString()+"\t"+ids[1]);
        		context.write(Key,Value);
        	}
        }
        }
    }

	
		

    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 2) {
            System.err.println("Usage: foaf <in> <out>");
            System.exit(2);
        }

        // create a job with name "foaf"
        Job job = new Job(conf, "foaf");
        job.setJarByClass(foaf.class);
        job.setSortComparatorClass(Compare.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

      
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        // set output key type
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}