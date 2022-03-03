package main;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Iterator;
import java.io.IOException;

// Notre classe REDUCE - templatée avec un type générique K pour la clé, un type de valeur IntWritable, et un type de retour
// (le retour final de la fonction Reduce) Text.
public class SalesAnalysisReduce extends
		Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	// La fonction REDUCE elle-même. Les arguments: la clé key (d'un type
	// générique K), un Iterable de toutes les valeurs
	// qui sont associées à la clé en question, et le contexte Hadoop (un handle
	// qui nous permet de renvoyer le résultat à Hadoop).
	public void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		// Pour parcourir toutes les valeurs associées à la clef fournie.
		Iterator<DoubleWritable> d = values.iterator();
		
		// Total (profit ou nombre d'unités vendues, en fonction du type de tâche à réaliser).
		double total = 0;
		
		// Pour chaque valeur...
		while (d.hasNext()) {
			// ... on l'ajoute au total.
			total += d.next().get();
		}
		
		// On renvoie le couple (clé;valeur) constitué de notre clé key et du total.
		context.write(key, new DoubleWritable(total));
	}
}