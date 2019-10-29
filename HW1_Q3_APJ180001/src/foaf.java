import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import javax.xml.bind.ValidationEvent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
    static HashMap<String, String> userData;

    public static class Map
            extends Mapper<LongWritable, Text, Text, Text>{
    	// Output (Key,Value) => (userIdPair,firstName:state)


        private Text keyText = new Text(); 
        private Text valueText = new Text(); 
        
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String line;
            userData = new HashMap<>();
            String dataPath=conf.get("dataPath");
            Path path = new Path("hdfs://localhost:9000"+dataPath);
            FileSystem fileSystem = FileSystem.get(conf);
            BufferedReader buffreredReader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));

            line=buffreredReader.readLine();

            while(line!=null){
                String splitLine[]=line.split(",");
                if(splitLine.length==10) {
                    String value = splitLine[1] + ":" + splitLine[5];
                    userData.put(splitLine[0], value);
                    line = buffreredReader.readLine();
                }
            }

        }
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] mydata = value.toString().split("\n");
            Configuration conf = context.getConfiguration();
            String id1=conf.get("id1");
            String id2= conf.get("id2");
            
            
            for (String data : mydata) {
            	String[] line=data.toString().split("\t");
            	if(line.length>1)
            	{
            		String userIdPair= line[0];
            		if(userIdPair.equals(id1+","+id2) || userIdPair.equals(id2+","+id1) )
            		{
            			String[] mFriendIds= line[1].toString().split(",");
            			if(userData!=null)
            				for (String mFriendId: mFriendIds) {
            					if(userData.containsKey(mFriendId))
            					{
            						String nameStatePair = userData.get(mFriendId);
            						keyText.set(userIdPair);
            						valueText.set(nameStatePair);
            						context.write(keyText, valueText);
            					}
            					
            				}
            			}
            		}	
            	}            	
            }
    }

    public static class Reduce
            extends Reducer<Text,Text,Text,Text> {
    	// Output (Key,Value) => (userIdPair, List<firstName:state>)

    	private Text valueText = new Text(); 
        
    	
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	String nameStatePairList="";
        	for (Text nameStatePair : values)
        	{
        		nameStatePairList+=","+nameStatePair;
        	}
        	nameStatePairList="["+nameStatePairList.substring(1)+"]";
        	valueText.set(nameStatePairList);
        	context.write(key, valueText);
       
        	            
           
        }
    }

	
		

    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 5) {
            System.err.println("Usage: foaf userId1 userId2 userDataFilePath <in> <out>");
            System.exit(2);
        }
        
        conf.set("id1",otherArgs[0].trim());
        conf.set("id2",otherArgs[1].trim());
        conf.set("dataPath",otherArgs[2].trim());
        
        // create a job with name "foaf"
        Job job = new Job(conf, "foaf");
        job.setJarByClass(foaf.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);


        // set output key type
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[3]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[4]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}