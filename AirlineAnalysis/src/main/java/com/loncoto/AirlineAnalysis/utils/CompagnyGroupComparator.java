package com.loncoto.AirlineAnalysis.utils;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompagnyGroupComparator extends WritableComparator {
	
	public CompagnyGroupComparator() {
		super(VolCompagnieCle.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		VolCompagnieCle cle1 = (VolCompagnieCle) a;
		VolCompagnieCle cle2 = (VolCompagnieCle)b;
		// je ne compare que le code compagnie et surtt pas le type (vol ou nom de compagie) comme sa hadoop regroupera ensemble
		// le cle nom et vols pour le mm compagnie et mon reducteur le recevra en mm tps
		return cle1.compagny_code.compareTo(cle2.compagny_code);
	}
	
	
}
