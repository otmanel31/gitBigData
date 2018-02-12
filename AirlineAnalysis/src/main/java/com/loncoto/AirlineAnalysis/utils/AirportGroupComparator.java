package com.loncoto.AirlineAnalysis.utils;

import javax.swing.text.StyledEditorKit.BoldAction;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AirportGroupComparator extends WritableComparator {

	public AirportGroupComparator() {
		super(VolAirportKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		VolAirportKey v1 = (VolAirportKey)a;
		VolAirportKey v2 = (VolAirportKey)b;
		
		// je ne compare que le code aerorprt et surtt pas le type (vol ou nom aeroport) comme sa hadoop regroupera ensemble
		// la cle (nom aeroportà) et le vols de cet aeorport a envoyer au reducer
		return v1.airport_code.compareTo(v2.airport_code);
	}
	
	

}
