package com.loncoto.AirlineAnalysis.utils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 
 * @author cloudera
 * 
 * choisi sur quel reducteur on envoie quel enregistrement
 *
 */
public class CompanyCodePartitionner extends Partitioner<VolCompagnieCle, Text> {

	// hadoop appelera la meth getPartition de notre partitionner pour decider vers quels reducteurs envoyer la donnee
	@Override
	public int getPartition(VolCompagnieCle cle, Text valeur, int nbPartition) {
		
		return (cle.compagny_code.hashCode() & Integer.MAX_VALUE) % nbPartition;
	}

}
