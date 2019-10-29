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
            extends Mapper<LongWritable, Text, Text, Text>{
    	// Output (Key,Value) => (userIdPair, friendList)
        private Text keyText = new Text(); 
        private Text valueText = new Text();
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] mydata = value.toString().split("\n");
            
            for (String data : mydata) {
            	String[] line=data.toString().split("\t");
            	if(line.length>1)
            	{
            		String userId= line[0];
                	String[] friendIds= line[1].toString().split(",");
                	for (String friend: friendIds)
                	{
                		if(Integer.parseInt(userId) > Integer.parseInt(friend))
                		{
                			keyText.set(friend+","+userId);
                			
                		}
                		else if(Integer.parseInt(userId) < Integer.parseInt(friend))
                		{	
                			keyText.set(userId+","+friend);
                		}
                		valueText.set(line[1]);
                		context.write(keyText, valueText);
                		
                	}
                	
            	}
            }
        }
    }

    public static class Reduce
            extends Reducer<Text,Text,Text,Text> {
    	// Output (Key,Value) => (userIdPair, mutualFriendList)

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
        	if(key.toString().equals("0,1") || key.toString().equals("20,28193") || key.toString().equals("1,29826") || key.toString().equals("6222,19272") || key.toString().equals("28041,28056"))
        	{
        		String[] strings=new String[2];
            	int i=0;
            	for (Text val: values)
            	{
            		strings[i]=val.toString();
            		i++;
            		
            	}
            	String[] list1 = strings[0].split(",");
            	String[] list2 = strings[1].split(",");
            	List <String> list= new ArrayList<String>();
            	
            	for (String element1: list1)
            	{
            		
            		for(String element2: list2)
            		{
            			if(element1.equals(element2))
            			{
            				list.add(element1);
            			}
            		}
            	}
            	
            	if(list.isEmpty())
            	{
            		
            	}
            	else
            	{
            		String x="";
            		for (String string : list) {
            			x+=","+string;
    				}
            		
            		 Text valueText = new Text(); 
            		 valueText.set(x.substring(1));
            		context.write(key,valueText);
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
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);

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