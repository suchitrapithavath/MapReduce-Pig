package edu.ucr.cs.cs226.spith001;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class KNN 
{
     public static class Mapperdistance extends Mapper<Object, Text, DoubleWritable, Text>
   {
      private Text point = new Text();
      private DoubleWritable dist = new DoubleWritable();
      public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
      {// mapper function
         Configuration config = context.getConfiguration();
         String line;
         double q_X=config.getDouble("queryX",0);// X data point value
         double q_Y=config.getDouble("queryY",0);// Y data point value
         line = value.toString();
         String[] result = line.split(",");//extracting the comma separated data points from file
         Double distance = Euclidiandistance(Double.parseDouble(result[1]),Double.parseDouble(result[2]),q_X,q_Y);//calling function to calculate the distance
         dist.set(distance);
         point.set(line);
         context.write(dist,point); //distance and data points as result of map function
      }
     public Double Euclidiandistance(Double lx, Double ly, Double qx, Double qy) {
			  return Math.sqrt(Math.pow(qx-lx, 2)+Math.pow(qy-ly,2));//function to calculate the distance between data points and query points 
   }
 }
   public static class KpointsReducer extends Reducer<DoubleWritable, Text, DoubleWritable,Text> 
   {
      private int counter=1;
      protected void setup(Context context)throws IOException, InterruptedException{
            counter=1;//initializing the counter variable
        }
      public void reduce(DoubleWritable distance, Iterable<Text> value, Context context) throws IOException, InterruptedException 
      {// reducer function
        try {
        int kn=context.getConfiguration().getInt("k",0);//getting the k value 
         if(counter <= kn) {//selecting the top k values

                for (Text points : value) {

                    context.write(distance,points);
                    counter++;

                }

            }
        }
        catch(Exception e)
        {
         System.out.println(e);
        }
      }
   }
   
   public static void main(String[] args) throws Exception 
   {
      try
      {
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "KNN");
      Double queryX= Double.parseDouble(args[1]);
      Double queryY= Double.parseDouble(args[2]);
      int k= Integer.parseInt(args[3]);
      job.getConfiguration().setDouble("queryx",queryX);
      job.getConfiguration().setDouble("queryY",queryY);
      job.getConfiguration().setInt("k",k);		
      job.setJarByClass(KNN.class);
      job.setInputFormatClass(TextInputFormat.class);
      job.setMapperClass(Mapperdistance.class);
      job.setReducerClass(KpointsReducer.class);
      job.setMapOutputKeyClass(DoubleWritable.class);
      job.setMapOutputValueClass(Text.class);		
      job.setOutputKeyClass(DoubleWritable.class);
      job.setOutputValueClass(Text.class);
		
      FileInputFormat.addInputPath(job, new Path(args[0]));//input file name
      FileOutputFormat.setOutputPath(job, new Path(args[4]));//output file name
		
      System.exit(job.waitForCompletion(true) ? 0 : 1);
     }

    catch (Exception e)
    {

      System.out.println(e);
   }
   }
}        
    


         

              

                 

        



