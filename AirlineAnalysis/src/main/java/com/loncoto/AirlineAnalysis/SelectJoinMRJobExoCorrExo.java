package com.loncoto.AirlineAnalysis;

import java.io.IOException;
import java.text.DecimalFormat;

import javax.swing.text.StyledEditorKit.BoldAction;

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

import com.loncoto.AirlineAnalysis.utils.AiportCodePartitioner;
import com.loncoto.AirlineAnalysis.utils.AiportCodePartitionerCorr;
import com.loncoto.AirlineAnalysis.utils.AirportGroupComparator;
import com.loncoto.AirlineAnalysis.utils.AirportGroupComparatorCorr;
import com.loncoto.AirlineAnalysis.utils.AirportSortComparator;
import com.loncoto.AirlineAnalysis.utils.AirportSortComparatorCorr;
import com.loncoto.AirlineAnalysis.utils.ArilineDataUtil;
import com.loncoto.AirlineAnalysis.utils.CompagnyGroupComparator;
import com.loncoto.AirlineAnalysis.utils.CompagnySortComparator;
import com.loncoto.AirlineAnalysis.utils.CompanyCodePartitionner;
import com.loncoto.AirlineAnalysis.utils.InfoVol;
import com.loncoto.AirlineAnalysis.utils.VolAeroportCleCorr;
import com.loncoto.AirlineAnalysis.utils.VolAirportKey;
import com.loncoto.AirlineAnalysis.utils.VolCompagnieCle;

/**
 * Hello world!
 * extend + implement pour mieu traiter les param en ligne de commande
 *  + implement nouvelle commande run venant de methode de tools cette methode demarera le job ou le lancement du map reduce
 */
public class SelectJoinMRJobExoCorrExo extends Configured implements Tool
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
	public static class VolMapper extends Mapper<LongWritable, Text, VolAeroportCleCorr, Text> {

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, VolAeroportCleCorr, Text>.Context context)
				throws IOException, InterruptedException {
			// sauter la ligne d'en-tête
			if (!ArilineDataUtil.isHeader(value)) {
				// informations du vol
				InfoVol infos = ArilineDataUtil.parseInfosVolsDelayFromText(value);
				// j'indique que j'envoie au reducteur un enregistrement type vol pour
				// telle compagnie aerienne (carrier)
				
				if (infos.status.get() == infos.NORMAL){
					VolAeroportCleCorr clef = 
							new VolAeroportCleCorr(VolAeroportCleCorr.TYPE_VOL, infos.aeroportDepart.toString());
					// ecriture vers le reducteur
					context.write(clef, ArilineDataUtil.infosVolToText(infos));
				}
			}
		}
	}
	
	// mapper fichier aeroport 
	public static class AirportMapper extends Mapper<LongWritable, Text, VolAeroportCleCorr, Text> {

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, VolAeroportCleCorr, Text>.Context context)
				throws IOException, InterruptedException {
			if (!ArilineDataUtil.isHeaderExo(value)) {
				String[] detailsAirport = ArilineDataUtil.parseAirportDetails(value);
				
				// generation clef pour reducteur
				VolAeroportCleCorr volAirportKey = new VolAeroportCleCorr(VolAeroportCleCorr.TYPE_AEROPORT, detailsAirport[0]);
				Text textInfo = new Text(detailsAirport[1] +", " + detailsAirport[5] +", " +detailsAirport[6]);
				
				context.write(volAirportKey, textInfo);
			}
		}
		
	}
	// jointure ds le reducer
	public static class MyReducer extends Reducer<VolAeroportCleCorr, Text, NullWritable, Text>{

		private String currentAirport = "mars one, 0, 0";
		
		@Override
		protected void reduce(VolAeroportCleCorr cle, Iterable<Text> infos,
				Reducer<VolAeroportCleCorr, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			
			double totalFlight = 0;
			double totalDepartureDelay = 0;
			double totalArrivalDelay = 0;
			
			for (Text info: infos){
				if (cle.typeRecord.get() == VolAeroportCleCorr.TYPE_AEROPORT){
					// jai recu un nom d aeroport
					this.currentAirport = info.toString();
				}else{
					// jai recu un vol
					
					InfoVol vol = ArilineDataUtil.textToInfoVol(info);
					if (vol.status.get() == InfoVol.NORMAL){
						totalFlight ++;
						totalDepartureDelay += (vol.retardDepart.get() > 0) ? vol.retardDepart.get() : 0;
						totalArrivalDelay += (vol.retardArrivee.get() > 0) ? vol.retardArrivee.get() : 0;
					}
					
					//context.write(NullWritable.get(), new Text("\""+this.compagnieCourante+"\"," + info.toString()));
				}
			}
			if (totalFlight > 0 ) {
				StringBuilder sb = new StringBuilder(this.currentAirport);
				DecimalFormat df = new DecimalFormat("0.0000");
				sb.append(",").append(" totalFlight: " + totalFlight)
				.append(", % retard arrivée: ").append(df.format( totalArrivalDelay/totalFlight)) // sans % a chacun de gerer les *100 .... 
				.append(", retard depart: " ).append(df.format(totalDepartureDelay/totalFlight))
				;
				context.write(NullWritable.get(), new Text(sb.toString()));
			}
		}
		
	}
	
    public static void main( String[] args ) throws Exception
    {
        System.out.println( "Hello World!" );
        
        Configuration conf = new Configuration();
        ToolRunner.run(new SelectJoinMRJobExoCorrExo(), args);
    }

	@Override
	public int run(String[] args) throws Exception {
		Job  job = Job.getInstance(getConf()); 
		
		job.setJarByClass(SelectJoinMRJobExoCorrExo.class);
		
		// format de fichier en entrant => cela ici ns importe peux vu qon faot des muttipleInput
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		// format de sortie du mapper => peux le dedure seul mais on le fait kan mm
		job.setMapOutputKeyClass(VolAeroportCleCorr.class);
		job.setMapOutputValueClass(Text.class);
		
		// classe permettant de mettre ds la config les args standars connu par hadoop pour -D avc clause etc 
		// ici nn necessaire
		//String[] arguments = new GenericOptionsParser(getConf(), args).getRemainingArgs(); 
		
		// Airport mapper soccupe des fichier de son repertoire donnee (contenant la liste des aeroports)
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, VolMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AirportMapper.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setSortComparatorClass(AirportSortComparatorCorr.class);
		job.setGroupingComparatorClass(AirportGroupComparatorCorr.class);
		
		job.setPartitionerClass(AiportCodePartitionerCorr.class);
		
		job.setReducerClass(MyReducer.class);
		
		job.setNumReduceTasks(1);
		
		boolean status = job.waitForCompletion(true); // true => verbose mode
		
		if (status) return 0;
		else return 1;
	}

    // run permet de rajjouter les option supplementaire en plus de quel est le mapper , reducteur etc .... 
	/*@Override 
	public int run(String[] args) throws Exception {

		
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
	}*/
}
