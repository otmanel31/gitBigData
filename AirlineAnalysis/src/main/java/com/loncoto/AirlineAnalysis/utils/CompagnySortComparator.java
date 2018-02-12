package com.loncoto.AirlineAnalysis.utils;

import org.apache.hadoop.io.WritableComparator;

// c un comparateur hadoop qui va ns  permettre de dire a hadoop comment trier les donnees entre le mapper et le reducteur
// ici en loccuirence on lui dit de comparer classiquement les vols compagnie cle 
public class CompagnySortComparator extends WritableComparator {

	public CompagnySortComparator() {
		super(VolCompagnieCle.class, true);
	}

}
