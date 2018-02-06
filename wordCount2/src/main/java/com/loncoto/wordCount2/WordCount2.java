package com.loncoto.wordCount2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Hello world!
 *
 */
public class WordCount2 
{
	//en api hadoop v2 on herite dune classe mapper directement 
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			String mot = value.toString();
			if (mot != null && !mot.trim().isEmpty()){
				// le contexte encapsule
				context.write(new Text(mot), new IntWritable(1));
			}
		}
		
	}
	
	// idem pour le reducteur en api v2 on herite dune simple classse reducer
	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

		// le context encapsule la sortie + reporter ... 
		@Override
		protected void reduce(Text cle, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int somme = 0;
			for (IntWritable val : values){
				somme += val.get();
				
			}
			context.write(cle, new IntWritable(somme));
		}
		
	}
	
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
        System.out.println( "Hello World!"  );
        // on passe maintenant par un objet Job ds lapi v2
        Job job = Job.getInstance(new Configuration());
        
        // association avc notre classe
        job.setJarByClass(WordCount2.class);
        
        // configuration de sortie
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // pas besoin de definir les type dentrees il le deduit du mapper
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        
        // definir les lecteurs  (fichier entree) et ecrivain (fichier sortie)
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        //  attention a prendre le bon inport pas le mm qu en v1
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        // lancement du job
        boolean status  = job.waitForCompletion(true);
        
        if (status){
        	System.exit(0); // tt c bien passer
        }else{
        	System.exit(1); // il y a eu probleme
        }
        
    }
}
