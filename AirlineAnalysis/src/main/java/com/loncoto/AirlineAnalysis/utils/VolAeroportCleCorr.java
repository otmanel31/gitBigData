package com.loncoto.AirlineAnalysis.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class VolAeroportCleCorr implements WritableComparable<VolAeroportCleCorr> {

	public static final int TYPE_VOL = 1;
	public static final int TYPE_AEROPORT = 0;
	
	public IntWritable typeRecord = new IntWritable();
	public Text airport_code = new Text();
	
	public VolAeroportCleCorr() {
		super();
	}

	public VolAeroportCleCorr(int typeRecord, String airport_code) {
		this.typeRecord.set(typeRecord);
		this.airport_code.set(airport_code);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.typeRecord.write(out);
		this.airport_code.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.typeRecord.readFields(in);
		this.airport_code.readFields(in);
	}

	@Override
	public int compareTo(VolAeroportCleCorr o) {
		// on compare dabor le code de laeroport et ensuite le vols
		int comparaison = this.airport_code.compareTo(o.airport_code);
		//si zero alors le trie ce fairt deja
		if (comparaison != 0) return comparaison;
		return this.typeRecord.compareTo(o.typeRecord);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((airport_code == null) ? 0 : airport_code.hashCode());
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
		VolAeroportCleCorr other = (VolAeroportCleCorr) obj;
		if (airport_code == null) {
			if (other.airport_code != null)
				return false;
		} else if (!airport_code.equals(other.airport_code))
			return false;
		if (typeRecord == null) {
			if (other.typeRecord != null)
				return false;
		} else if (!typeRecord.equals(other.typeRecord))
			return false;
		return true;
	}
	
	
	
	

}
