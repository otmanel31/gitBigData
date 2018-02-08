package com.loncoto.AirlineAnalysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
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
public class SelectWhereMRJob extends Configured implements Tool
{
	
	// select avc where donc pas de group by donc on fait jusdte un mapper ici ss reducteur
	public static class MyMapper extends Mapper<LongWritable, Text, NullWritable, Text>{

		private  int delayInMinutes = 0;
		
		private int distance = 0;
		
		@Override
		protected void setup(
				Mapper<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			// a lexecution grace au generic option parser on peux renseigner ce parametre avc -D map.where.delay = ???
			this.delayInMinutes = context.getConfiguration().getInt("map.where.delay", 1);
			this.distance = context.getConfiguration().getInt("map.where.distance", 1);
		}



		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			// sauter mla 1ere ligne
			 if( !ArilineDataUtil.isHeader(value) ){
				 // extrait les champs de la ligne
				 String[] champs = ArilineDataUtil.getSelectedColumnsA(value);
				 // on verifize pour ne garder que les leigne ou le depart retard est superieurt a 15
				 // parse minute a voir comme un parse int qui gere les valeur NA ( not available) du fichier csv
				 if (ArilineDataUtil.parseMinutes(champs[8], 0) > this.delayInMinutes && ArilineDataUtil.parseMinutes(champs[5], 0) > this.distance){
					 StringBuilder sb = ArilineDataUtil.mergeStringArray(champs, ",");
					 context.write(NullWritable.get(), new Text(sb.toString()));
				 }
			 }
		}
		
	}
	
    public static void main( String[] args ) throws Exception
    {
        System.out.println( "Hello World!" );
        
        Configuration conf = new Configuration();
        ToolRunner.run(new SelectWhereMRJob(), args);
    }

    // run permet de rajjouter les option supplementaire en plus de quel est le mapper , reducteur etc .... 
	@Override 
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Job job = Job.getInstance(getConf()); // get conf pour recuperer la conf depuis mla classe parent ... inutile de faire un new conf() ... 
		
		job.setJarByClass(SelectWhereMRJob.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(MyMapper.class);
		// ici on lui dit pas de reducteur sinon met redcuteur par defaut ce qui predn du tps pour rien
		job.setNumReduceTasks(0);
		
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
