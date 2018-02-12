package com.loncoto.AirlineAnalysis.utils;

import javax.swing.text.StyledEditorKit.BoldAction;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AirportGroupComparatorCorr extends WritableComparator {

	public AirportGroupComparatorCorr() {
		super(VolAeroportCleCorr.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		VolAeroportCleCorr v1 = (VolAeroportCleCorr)a;
		VolAeroportCleCorr v2 = (VolAeroportCleCorr)b;
		
		// je ne compare que le code aerorprt et surtt pas le type (vol ou nom aeroport) comme sa hadoop regroupera ensemble
		// la cle (nom aeroportà) et le vols de cet aeorport a envoyer au reducer
		return v1.airport_code.compareTo(v2.airport_code);
	}
	
	

}
