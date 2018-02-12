package com.loncoto.AirlineAnalysis.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class VolAirportKey implements WritableComparable<VolAirportKey> {
	
	public static final int TYPE_VOL = 1;
	public static final int TYPE_AEROPORT = 0;
	
	public IntWritable type_record = new IntWritable();
	public Text airport_code = new Text();
	
	

	public VolAirportKey() {
		super();
	}

	public VolAirportKey(int type_record, String airport_code) {

		this.type_record.set(type_record);;
		this.airport_code.set(airport_code); 
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.type_record.write(out);
		this.airport_code.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.type_record.readFields(in);
		this.airport_code.readFields(in);
		
	}

	@Override
	public int compareTo(VolAirportKey o) {
		int comparaison = this.airport_code.compareTo(o.airport_code); // compare to retourne 0 si egale -1 si inferieur +1 si superieur
		if (comparaison != 0 ) return comparaison;
		return this.type_record.compareTo(o.type_record);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((airport_code == null) ? 0 : airport_code.hashCode());
		result = prime * result
				+ ((type_record == null) ? 0 : type_record.hashCode());
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
		VolAirportKey other = (VolAirportKey) obj;
		if (airport_code == null) {
			if (other.airport_code != null)
				return false;
		} else if (!airport_code.equals(other.airport_code))
			return false;
		if (type_record == null) {
			if (other.type_record != null)
				return false;
		} else if (!type_record.equals(other.type_record))
			return false;
		return true;
	}

	
	
}
