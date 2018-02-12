package com.loncoto.AirlineAnalysis.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

// quand on imlement writable => il saura serilaiser 
public class InfoVol implements Writable {
	
	public static final int NORMAL = 0;
	public static final int CANCELLED = 1;
	public static final int DIVERTED = 2;
	
	
	
	// classse regroupant tt les infos 
	public IntWritable annee = new IntWritable();
	public IntWritable mois = new IntWritable();
	public IntWritable date = new IntWritable();
	public IntWritable retardDepart = new IntWritable();
	public IntWritable retardArrivee = new IntWritable();
	public Text	 aeroportDepart = new Text();
	public Text aeroportArrivee = new Text();
	public Text compagnie = new Text();
	public IntWritable status = new IntWritable();
	
	// ecrire ds flux un objet infosVOls
	@Override
	public void write(DataOutput out) throws IOException {
		this.annee.write(out);
		this.mois.write(out);
		this.date.write(out);
		this.retardDepart.write(out);
		this.retardArrivee.write(out);
		this.aeroportDepart.write(out);
		this.aeroportArrivee.write(out);
		this.compagnie.write(out);
		this.status.write(out);
	}
	
	// lire depus flux binaire un objet infosVOls
	@Override
	public void readFields(DataInput in) throws IOException {
		this.annee.readFields(in);
		this.mois.readFields(in);
		this.date.readFields(in);
		this.retardDepart.readFields(in);
		this.retardArrivee.readFields(in);
		this.aeroportDepart.readFields(in);
		this.aeroportArrivee.readFields(in);
		this.compagnie.readFields(in);
		this.status.readFields(in);
	}

	
}
