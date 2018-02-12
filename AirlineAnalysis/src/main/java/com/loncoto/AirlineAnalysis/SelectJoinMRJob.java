package com.loncoto.AirlineAnalysis;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.loncoto.AirlineAnalysis.utils.ArilineDataUtil;
import com.loncoto.AirlineAnalysis.utils.CompagnyGroupComparator;
import com.loncoto.AirlineAnalysis.utils.CompagnySortComparator;
import com.loncoto.AirlineAnalysis.utils.CompanyCodePartitionner;
import com.loncoto.AirlineAnalysis.utils.InfoVol;
import com.loncoto.AirlineAnalysis.utils.VolCompagnieCle;

/**
 * Hello world!
 * extend + implement pour mieu traiter les param en ligne de commande
 *  + implement nouvelle commande run venant de methode de tools cette methode demarera le job ou le lancement du map reduce
 */
public class SelectJoinMRJob extends Configured implements Tool
{
	
	public static final IntWritable FLIGHT = new IntWritable(0);
	public static final IntWritable DEPARTURE_DELAY = new IntWritable(1);
	public static final IntWritable ARRIVAL_DELAY = new IntWritable(2);
	public static final IntWritable DEPARTURE_ONTIME = new IntWritable(3);
	public static final IntWritable ARRIVAL_ONTIME = new IntWritable(4);
	public static final IntWritable CANCELLED = new IntWritable(5);
	public static final IntWritable DIVERTED = new IntWritable(6);
	
	// select avc where donc pas de group by donc on fait jusdte un mapper ici ss reducteur

	// mapper qui s'occupe des fichiers contenant les vols
	public static class VolMapper extends Mapper<LongWritable, Text, VolCompagnieCle, Text> {

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, VolCompagnieCle, Text>.Context context)
				throws IOException, InterruptedException {
			// sauter la ligne d'en-tête
			if (!ArilineDataUtil.isHeader(value)) {
				// informations du vol
				InfoVol infos = ArilineDataUtil.parseInfosVolsDelayFromText(value);
				// j'indique que j'envoie au reducteur un enregistrement type vol pour
				// telle compagnie aerienne (carrier)
				VolCompagnieCle clef = 
						new VolCompagnieCle(VolCompagnieCle.TYPE_VOL, infos.compagnie.toString());
				// ecriture vers le reducteur
				context.write(clef, ArilineDataUtil.infosVolToText(infos));
			}
		}
	}
	
	// mapper qui s'occupe des fichiers contenant les vols
	public static class CompagnieMapper extends Mapper<LongWritable, Text, VolCompagnieCle, Text> {

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, VolCompagnieCle, Text>.Context context)
				throws IOException, InterruptedException {
			
				String[] detailsCompagnie = ArilineDataUtil.parseCompanyDetails(value);
				// je genere la clef pour le reducteur
				VolCompagnieCle clef = 
						new VolCompagnieCle(VolCompagnieCle.TYPE_COMPAGNY, detailsCompagnie[0].trim());
				Text infocompagnie = new Text(detailsCompagnie[1]);
				context.write(clef, infocompagnie);
		}
	}
	
	public static class MyReducer extends Reducer<VolCompagnieCle, Text, NullWritable, Text>{

		private String compagnieCourante = "inconnue";
		
		@Override
		protected void reduce(VolCompagnieCle cle, Iterable<Text> infos,
				Reducer<VolCompagnieCle, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			
			double totalFlight = 0;
			double totalCancelled = 0;
			double totalDiverted = 0;
			/*double totalDepartureOnTime = 0;
			double totalDepartureDelay = 0;
			double totalArrivalOnTime = 0;
			double totalArrivalDelay = 0;*/
			
			for (Text info: infos){
				if (cle.typeRecord.get() == VolCompagnieCle.TYPE_COMPAGNY){
					// jai recu un nom de compagnie
					this.compagnieCourante = info.toString();
				}else{
					// jai recu un vol
					totalFlight ++;
					InfoVol vol = ArilineDataUtil.textToInfoVol(info);
					if (vol.status.get() == InfoVol.CANCELLED) totalCancelled ++;
					else if (vol.status.get() == InfoVol.DIVERTED) totalDiverted ++;
					
					//context.write(NullWritable.get(), new Text("\""+this.compagnieCourante+"\"," + info.toString()));
				}
			}
			if (totalFlight > 0 ) {
				StringBuilder sb = new StringBuilder(this.compagnieCourante);
				DecimalFormat df = new DecimalFormat("0.0000");
				sb.append(",").append(" totalFlight: " + totalFlight)
				.append(", totalCancelled: ").append(df.format( totalCancelled/totalFlight)) // sans % a chacun de gerer les *100 .... 
				.append(", totalDiverted: " ).append(df.format(totalDiverted/totalFlight))
				;
				context.write(NullWritable.get(), new Text(sb.toString()));
			}
			/*
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
			context.write(NullWritable.get(), new Text( sb.toString()));*/
		}
		
	}
	
    public static void main( String[] args ) throws Exception
    {
        System.out.println( "Hello World!" );
        
        Configuration conf = new Configuration();
        ToolRunner.run(new SelectJoinMRJob(), args);
    }

    // run permet de rajjouter les option supplementaire en plus de quel est le mapper , reducteur etc .... 
	@Override 
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Job job = Job.getInstance(getConf()); // get conf pour recuperer la conf depuis mla classe parent ... inutile de faire un new conf() ... 
		
		job.setJarByClass(SelectJoinMRJob.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		// format de sortie du mapper peut le deduire seul mais on le met au cas ou future evol
		job.setMapOutputKeyClass(VolCompagnieCle.class);
		job.setMapOutputValueClass(Text.class);
		
		
		
		
		
		// cett classe permet de ùettre ds la configuration les arguments standard connu par haddop
		// en ou renvoyant ensuite les autres args restant
		String[] arguments = new GenericOptionsParser(getConf(), args).getRemainingArgs();
		
		// le vol mapper soccupe des fichier ds ce repertoire
		MultipleInputs.addInputPath(job, new Path(arguments[0]), TextInputFormat.class, VolMapper.class);
		MultipleInputs.addInputPath(job, new Path(arguments[1]), TextInputFormat.class, CompagnieMapper.class);
		
		FileOutputFormat.setOutputPath(job, new Path(arguments[2]));
		
		job.setSortComparatorClass(CompagnySortComparator.class);
		job.setGroupingComparatorClass(CompagnyGroupComparator.class);
		
		job.setReducerClass(MyReducer.class);
		 
		job.setPartitionerClass(CompanyCodePartitionner.class);
		
		job.setNumReduceTasks(3);
		// true verbose ds la console
		boolean status = job.waitForCompletion(true);
		
		if (status) return 0;
		else return 1;
	}
}
