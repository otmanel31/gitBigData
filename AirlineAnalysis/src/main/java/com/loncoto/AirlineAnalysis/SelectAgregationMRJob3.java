package com.loncoto.AirlineAnalysis;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.loncoto.AirlineAnalysis.utils.ArilineDataUtil;

/**
 * Hello world!
 * extend + implement pour mieu traiter les param en ligne de commande
 *  + implement nouvelle commande run venant de methode de tools cette methode demarera le job ou le lancement du map reduce
 */
public class SelectAgregationMRJob3 extends Configured implements Tool
{
	
	public static final IntWritable ZERO_TO_DEUX_CENT = new IntWritable(0);
	public static final IntWritable DEUX_CENT_TO_QUATRE_CENT = new IntWritable(1);
	public static final IntWritable QUATRE_CENT_TO_HUIT_CENT = new IntWritable(2);
	public static final IntWritable PLUS_HUIT_CENT = new IntWritable(3);
	
	// select avc where donc pas de group by donc on fait jusdte un mapper ici ss reducteur
	public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, BooleanWritable>{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, IntWritable, BooleanWritable>.Context context)
				throws IOException, InterruptedException {
			// sauter mla 1ere ligne
			 if( !ArilineDataUtil.isHeader(value) ){
				 // extrait les champs de la ligne
				 String[] champs = ArilineDataUtil.getSelectedColumnsC(value);
				 String carrier = champs[12];
				 int distance = ArilineDataUtil.parseMinutes(champs[5],0);
				 
				 boolean enRetard = ArilineDataUtil.parseMinutes(champs[9], 0)> 15;
				 
				 if (distance < 200) context.write(ZERO_TO_DEUX_CENT, new BooleanWritable(enRetard));
				 else if (distance <= 400 ) context.write(DEUX_CENT_TO_QUATRE_CENT,new BooleanWritable(enRetard));
				 else if (distance <= 800 ) context.write(QUATRE_CENT_TO_HUIT_CENT, new BooleanWritable(enRetard));
				 else context.write(PLUS_HUIT_CENT, new BooleanWritable(enRetard));
				 
			 }
		}
		
	}
	
	public static class MyReducer extends Reducer<IntWritable, BooleanWritable, NullWritable, Text>{

		@Override
		protected void reduce(IntWritable dist, Iterable<BooleanWritable> enRetards,
				Reducer<IntWritable, BooleanWritable, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			double zeroToCent = 0;
			double deuxCentTo4 = 0;
			double quatreCentTo8 = 0;
			double plus8 = 0;
			
			double totalFlight = 0;
			double enRetard = 0;
			double ponctuel = 0;
			
			for (BooleanWritable d: enRetards){
				totalFlight += 1;
				if (d.get()) enRetard ++;
				else ponctuel ++;
				
			}
			
			StringBuilder sb = new StringBuilder("distnace: " + dist.toString());
			// class permattat de formater proprement des decimales
			DecimalFormat df = new DecimalFormat("0.0000");
			
			sb.append(",").append(" totalFlight: " + totalFlight)
				.append(",retard : ").append(df.format( enRetard/totalFlight)) // sans % a chacun de gerer les *100 .... 
				.append(", ponctuel : ").append(df.format( ponctuel/totalFlight))
			;
			context.write(NullWritable.get(), new Text( sb.toString()));
		}
		
	}
	
    public static void main( String[] args ) throws Exception
    {
        System.out.println( "Hello World!" );
        
        Configuration conf = new Configuration();
        ToolRunner.run(new SelectAgregationMRJob3(), args);
    }

    // run permet de rajjouter les option supplementaire en plus de quel est le mapper , reducteur etc .... 
	@Override 
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Job job = Job.getInstance(getConf()); // get conf pour recuperer la conf depuis mla classe parent ... inutile de faire un new conf() ... 
		
		job.setJarByClass(SelectAgregationMRJob3.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		// format de sortie du mapper peut le deduire seul mais on le met au cas ou future evol
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(BooleanWritable.class);
		
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setNumReduceTasks(3);
		
		// cett classe permet de ùettre ds la configuration les arguments standard connu par haddop
		// en ou renvoyant ensuite les autres args restant
		String[] arguments = new GenericOptionsParser(getConf(), args).getRemainingArgs();
		
		// chemin de mlecture ds hdfs
		FileInputFormat.setInputPaths(job, new Path(arguments[0]));
		// chemin de decirture ds hdfs
		FileOutputFormat.setOutputPath(job, new Path(arguments[1]));
		
		// true verbose ds la console
		boolean status = job.waitForCompletion(true);
		
		if (status) return 0;
		else return 1;
	}
}
