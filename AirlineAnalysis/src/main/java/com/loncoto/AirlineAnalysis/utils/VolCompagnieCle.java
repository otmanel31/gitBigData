package com.loncoto.AirlineAnalysis.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


// class non natif comme TExt oi INtwritable qui impleme,nte automatiquement comparable , 
// dans notre cas ici c a ns de le faire
public class VolCompagnieCle implements WritableComparable<VolCompagnieCle> {

	public static final int TYPE_VOL = 1;
	public static final int TYPE_COMPAGNY = 2;
	
	public IntWritable typeRecord = new IntWritable();
	public Text compagny_code = new Text();
	
	// (type record = 0, compagny_code = "ps") ----> (le nom de la compagnie)
	// (type record = 1, compagny_code = "ps") ----> (le vol de la compagnie)
		
	public VolCompagnieCle(int typeRecord, String compagny_code) { // remplace intwritaablle par int et text par string pour passer les valeurs directements
		super();
		this.typeRecord.set(typeRecord);
		this.compagny_code.set(compagny_code);
	}
	
	public VolCompagnieCle() {}

	
	@Override
	public void write(DataOutput out) throws IOException {
		this.typeRecord.write(out);
		this.compagny_code.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.typeRecord.readFields(in);
		this.compagny_code.readFields(in);
	}

	@Override
	public int compareTo(VolCompagnieCle o) {
		int comparaison = this.compagny_code.compareTo(o.compagny_code); // compare to retourne 0 si egale -1 si inferieur +1 si superieur
		if (comparaison != 0) return comparaison;
		// tri de base par ode compagnie (en locurrence ordre alphabetique)
		
		//si jamais des deux clefgs concerne la meme compagnie
		// envoyer dabord le nom de la compagnie et ensuite les vols
		// type = TYPE compagnie < type = vols
		return this.typeRecord.compareTo(o.typeRecord);
	}

	// gener hascode et equal pour comparer si deux objet son egaux ou pas 
	// de tps en tps haddop controle que deux objet sont egaux dou necessité de gen ce deux meth
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((compagny_code == null) ? 0 : compagny_code.hashCode());
		result = prime * result
				+ ((typeRecord == null) ? 0 : typeRecord.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		VolCompagnieCle other = (VolCompagnieCle) obj;
		if (compagny_code == null) {
			if (other.compagny_code != null)
				return false;
		} else if (!compagny_code.equals(other.compagny_code))
			return false;
		if (typeRecord == null) {
			if (other.typeRecord != null)
				return false;
		} else if (!typeRecord.equals(other.typeRecord))
			return false;
		return true;
	}
	
}
