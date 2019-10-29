import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import javax.swing.plaf.SliderUI;
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
    static HashMap<String, String> friendAgeData;
    static HashMap<String, String> userAddressData;
    public static class Map
            extends Mapper<LongWritable, Text, FloatWritable, Text>{
    	// Output (Key,Value) => (average, userId)


        private FloatWritable keyFloat = new FloatWritable();
        private Text valueText = new Text(); 
        
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String line;
            friendAgeData = new HashMap<>();
            String dataPath=conf.get("dataPath");
            Path path = new Path("hdfs://localhost:9000"+dataPath);
            FileSystem fileSystem = FileSystem.get(conf);
            BufferedReader buffreredReader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));

            line=buffreredReader.readLine();

            while(line!=null){
                String splitLine[]=line.split(",");
                if(splitLine.length==10) {
                	String birthDate = splitLine[9];
                	try {
						
						Date birthDay= new SimpleDateFormat("MM/dd/yyyy").parse(birthDate);
						LocalDate localBirthDay= birthDay.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
					    int age= Period.between(localBirthDay, LocalDate.now()).getYears();
					    
	                    friendAgeData.put(splitLine[0], ""+age);
	                    
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						System.out.println("date error");
						e.printStackTrace();
					}  
                    
                }
                line = buffreredReader.readLine();
            }
        }
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] mydata = value.toString().split("\n");
            
            float count=0;
            float sum=0;
            int age=0;
            for (String data : mydata) 
            {
            	String[] line=data.toString().split("\t");
            	if(line.length>1)
            	{
            		String userId= line[0];
            		String[] mFriendIds= line[1].toString().split(",");
        			if(friendAgeData!=null)
        			{
        				for (String mFriendId: mFriendIds) {
        					if(friendAgeData.containsKey(mFriendId))
        					{
        						age = Integer.parseInt(friendAgeData.get(mFriendId));
        						sum+=age;
        						count+=1;
        					}
        					
        				}
        				float avg=sum/count;
        				System.out.println(avg);
        				keyFloat.set(avg);
						valueText.set(userId);
						context.write(keyFloat, valueText);
        			}
    			}
            			
        	} 
        }
    }

    public static class Compare extends WritableComparator {
        public Compare(){
            super(FloatWritable.class, true);
        }
        @Override
        public int compare(WritableComparable wc1, WritableComparable wc2) {
            FloatWritable k1 = (FloatWritable) wc1;
            FloatWritable k2 = (FloatWritable) wc2;

            int res=k1.compareTo(k2)*(-1);
            return res;
        }
    }

    public static class Reduce
            extends Reducer<FloatWritable,Text,Text,Text> {
    	// Output (Key,Value) => (userId, address,average.)

    	private Text valueText = new Text(); 
    	private Text keyText = new Text();
    	int count=0;
    	
    	public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String line;
            userAddressData = new HashMap<>();
            String dataPath=conf.get("dataPath");
            Path path = new Path("hdfs://localhost:9000"+dataPath);
            FileSystem fileSystem = FileSystem.get(conf);
            BufferedReader buffreredReader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));

            line=buffreredReader.readLine();

            while(line!=null){
                String splitLine[]=line.split(",");
                if(splitLine.length==10) {
                	String address = splitLine[3]+','+splitLine[4]+","+splitLine[5];
                	userAddressData.put(splitLine[0], address);
                }
                line = buffreredReader.readLine();
            }
        }

        
    	
        public void reduce(FloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	String address;
        	String value;
        	for (Text userId : values)
        	{
        		if(count == 15)
        		{
        			break;
        		}
        		else {
					count+=1;
        			if(userAddressData.containsKey(userId.toString()))
    				{
    					address = userAddressData.get(userId.toString());
    					value = address+","+key.toString()+".";
    					keyText.set(userId);
    					valueText.set(value);
    					context.write(keyText, valueText);
    				
    				}
        		}	
        	}        	            
        }
    }

    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 3) {
            System.err.println("Usage: foaf userDataFilePath <in> <out>");
            System.exit(2);
        }
        
        conf.set("dataPath",otherArgs[0].trim());
        
        // create a job with name "foaf"
        Job job = new Job(conf, "foaf");
        job.setJarByClass(foaf.class);
        job.setSortComparatorClass(Compare.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(FloatWritable.class);
        job.setMapOutputValueClass(Text.class);
        // set output key type
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}