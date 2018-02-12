package com.loncoto.AirlineAnalysis.utils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AiportCodePartitionerCorr extends Partitioner<VolAeroportCleCorr, Text> {

	@Override
	public int getPartition(VolAeroportCleCorr cle, Text valeur, int nbPartition) {

		// hadoop appelera la meth getPartition de notre partitionner pour decider vers quels reducteurs envoyer la donnee
		return (cle.airport_code.hashCode() & Integer.MAX_VALUE ) % nbPartition;
	}

}
