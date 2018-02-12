package com.loncoto.AirlineAnalysis;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
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
public class SelectAgregationCombinerMRJob2 extends Configured implements Tool
{
	
	public static final IntWritable FLIGHT = new IntWritable(0);
	public static final IntWritable DEPARTURE_DELAY = new IntWritable(1);
	public static final IntWritable ARRIVAL_DELAY = new IntWritable(2);
	public static final IntWritable DEPARTURE_ONTIME = new IntWritable(3);
	public static final IntWritable ARRIVAL_ONTIME = new IntWritable(4);
	public static final IntWritable CANCELLED = new IntWritable(5);
	public static final IntWritable DIVERTED = new IntWritable(6);
	// TYPE POUR LA MAP
	public static final IntWritable TYPE = new IntWritable(0);
	public static final IntWritable VALUE = new IntWritable(1);
	
	// select avc where donc pas de group by donc on fait jusdte un mapper ici ss reducteur
	public static class MyMapper extends Mapper<LongWritable, Text, Text, MapWritable>{

		private MapWritable getMapWritable(IntWritable type, IntWritable value){
			MapWritable m = new MapWritable();
			m.put(TYPE, type);
			m.put(VALUE, value);
			return m;
		}

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, MapWritable>.Context context)
				throws IOException, InterruptedException {
			// sauter mla 1ere ligne
			 if( !ArilineDataUtil.isHeader(value) ){
				 // extrait les champs de la ligne
				 String[] champs = ArilineDataUtil.getSelectedColumnsB(value);
				 String month = champs[0];
				 int delayArrival = ArilineDataUtil.parseMinutes(champs[9],0);
				 int delayDeparture = ArilineDataUtil.parseMinutes(champs[8],0);
				 boolean isCancelled = ArilineDataUtil.parseBoolean(champs[10],false) ;
				 boolean isDIverted = ArilineDataUtil.parseBoolean(champs[11],false) ;
				 
				 // compter un vol
				 context.write(new Text(month), getMapWritable(FLIGHT, new IntWritable(1)));
				 
				 // verifie sil est annuler dabord comme sa sa sert a rien de faire le reste
				 if (isCancelled)  context.write(new Text(month), getMapWritable(CANCELLED, new IntWritable(1)));
				 else if (isDIverted)  context.write(new Text(month), getMapWritable(DIVERTED, new IntWritable(1)));
				 else{
					 // retard ou a lheure
					 if (delayArrival >= 10 )  context.write(new Text(month), getMapWritable(ARRIVAL_DELAY, new IntWritable(1)));
					 else  context.write(new Text(month), getMapWritable(ARRIVAL_ONTIME, new IntWritable(1)));
					 
					 if (delayDeparture >= 10 )  context.write(new Text(month), getMapWritable(DEPARTURE_DELAY, new IntWritable(1)));
					 else context.write(new Text(month), getMapWritable(DEPARTURE_ONTIME, new IntWritable(1)));
				 } 
			 }
		}
	}
	
	public static class MyCombiner extends Reducer<Text, MapWritable, Text, MapWritable>{

		private MapWritable getMapWritable(IntWritable type, IntWritable value){
			MapWritable m = new MapWritable();
			m.put(TYPE, type);
			m.put(VALUE, value);
			return m;
		}
		@Override
		protected void reduce(Text month, Iterable<MapWritable> codes,
				Reducer<Text, MapWritable, Text, MapWritable>.Context context)
				throws IOException, InterruptedException {
			int totalFlight = 0;
			int totalCancelled = 0;
			int totalDiverted = 0;
			int totalDepartureOnTime = 0;
			int totalDepartureDelay = 0;
			int totalArrivalOnTime = 0;
			int totalArrivalDelay = 0;
			
			for (MapWritable code: codes){
				// ttention pas de == car ce nestv pas un int de base
				IntWritable type = (IntWritable)code.get(TYPE);
				IntWritable value = (IntWritable)code.get(VALUE);
				
				if (type.equals(FLIGHT)) totalFlight += value.get();
				else if (type.equals(CANCELLED)) totalCancelled += value.get();
				else if (type.equals(DIVERTED)) totalDiverted += value.get();
				else if (type.equals(DEPARTURE_ONTIME)) totalDepartureOnTime += value.get();
				else if (type.equals(DEPARTURE_DELAY)) totalDepartureDelay += value.get();
				else if (type.equals(ARRIVAL_ONTIME)) totalArrivalOnTime += value.get();
				else if (type.equals(ARRIVAL_DELAY)) totalArrivalDelay += value.get();
			}
			
			context.write(new Text(month.toString()), getMapWritable(FLIGHT, new IntWritable(totalFlight)) );
			context.write(new Text(month.toString()), getMapWritable(CANCELLED, new IntWritable(totalCancelled)) );
			context.write(new Text(month.toString()), getMapWritable(DIVERTED, new IntWritable(totalDiverted)) );
			context.write(new Text(month.toString()), getMapWritable(DEPARTURE_ONTIME, new IntWritable(totalDepartureOnTime)) );
			context.write(new Text(month.toString()), getMapWritable(DEPARTURE_DELAY, new IntWritable(totalDepartureDelay)) );
			context.write(new Text(month.toString()), getMapWritable(ARRIVAL_ONTIME, new IntWritable(totalArrivalOnTime)) );
			context.write(new Text(month.toString()), getMapWritable(ARRIVAL_DELAY, new IntWritable(totalArrivalDelay)) );
		}
		
	}
	
	public static class MyReducer extends Reducer<Text, MapWritable, NullWritable, Text>{

		@Override
		protected void reduce(Text month, Iterable<MapWritable> codes,
				Reducer<Text, MapWritable, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			double totalFlight = 0;
			double totalCancelled = 0;
			double totalDiverted = 0;
			double totalDepartureOnTime = 0;
			double totalDepartureDelay = 0;
			double totalArrivalOnTime = 0;
			double totalArrivalDelay = 0;
			
			for (MapWritable code: codes){
				// ttention pas de == car ce nestv pas un int de base
				IntWritable type = (IntWritable)code.get(TYPE);
				IntWritable value = (IntWritable)code.get(VALUE);
				
				if (type.equals(FLIGHT)) totalFlight += value.get();
				else if (type.equals(CANCELLED)) totalCancelled += value.get();
				else if (type.equals(DIVERTED)) totalDiverted += value.get();
				else if (type.equals(DEPARTURE_ONTIME)) totalDepartureOnTime += value.get();
				else if (type.equals(DEPARTURE_DELAY)) totalDepartureDelay += value.get();
				else if (type.equals(ARRIVAL_ONTIME)) totalArrivalOnTime += value.get();
				else if (type.equals(ARRIVAL_DELAY)) totalArrivalDelay += value.get();
			}
			StringBuilder sb = new StringBuilder(month.toString());
			// class permattat de formater proprement des decimales
			DecimalFormat df = new DecimalFormat("0.0000");
			
			sb.append(",").append(" totalFlight: " + totalFlight)
				.append(", totalCancelled: ").append(df.format( totalCancelled/totalFlight)) // sans % a chacun de gerer les *100 .... 
				.append(", totalDiverted: " ).append(df.format(totalDiverted/totalFlight))
				.append(", totalDepartureOnTime: ").append(df.format(totalDepartureOnTime/totalFlight))
				.append(", totalDepartureDelay: ").append(df.format(totalDepartureDelay/totalFlight))
				.append(", totalArrivalOnTime: ").append(df.format(totalArrivalOnTime/totalFlight))
				.append(", totalArrivalDelay: ").append(df.format(totalArrivalDelay/totalFlight))
			;
			context.write(NullWritable.get(), new Text( sb.toString()));
		}
		
	}
	
    public static void main( String[] args ) throws Exception
    {
        System.out.println( "Hello World!" );
        
        Configuration conf = new Configuration();
        ToolRunner.run(new SelectAgregationCombinerMRJob2(), args);
    }

    // run permet de rajjouter les option supplementaire en plus de quel est le mapper , reducteur etc .... 
	@Override 
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Job job = Job.getInstance(getConf()); // get conf pour recuperer la conf depuis mla classe parent ... inutile de faire un new conf() ... 
		
		job.setJarByClass(SelectAgregationCombinerMRJob2.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		// format de sortie du mapper peut le deduire seul mais on le met au cas ou future evol
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);
		
		
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyCombiner.class);
		job.setReducerClass(MyReducer.class);
		
		job.setNumReduceTasks(1);
		
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
