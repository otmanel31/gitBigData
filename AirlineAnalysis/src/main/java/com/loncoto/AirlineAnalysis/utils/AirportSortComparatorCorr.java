package com.loncoto.AirlineAnalysis.utils;

import org.apache.hadoop.io.WritableComparator;

// c un comparateur hadoop qui va ns  permettre de dire a hadoop comment trier les donnees entre le mapper et le reducteur
// ici en loccuirence on lui dit de comparer classiquement les vols compagnie cle 
public class AirportSortComparatorCorr extends WritableComparator {

	public AirportSortComparatorCorr() {
		super(VolCompagnieCle.class, true); // super du constructeur du parent ....  si on le met pas(par défaut a false) utiliser la version optimiser sinon ici on lui dit instancie
	}

}
