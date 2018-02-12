package com.loncoto.AirlineAnalysis.utils;

import org.apache.hadoop.io.WritableComparator;

public class AirportSortComparator extends WritableComparator {

	public AirportSortComparator() {
		super(VolAeroportCleCorr.class, true); // true => utilise le compare des objet et comparaison binaire( byte to byte ...)
	}
	

}
