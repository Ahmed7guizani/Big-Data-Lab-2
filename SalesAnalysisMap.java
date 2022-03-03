package main;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

// Notre classe MAP.
public class SalesAnalysisMap extends Mapper<Object, Text, Text, DoubleWritable> {
	// La fonction MAP elle-même.
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// On récupère le type de tâche à réaliser et son argument dans la configuration du job.
		Configuration conf = context.getConfiguration();
		String taskName = conf.getStrings("taskName", "")[0];
		String taskArg = conf.getStrings("taskArg", "")[0];
		
		// On ignore les lignes inutiles du fichier, à savoir :
		//   - les premières lignes : le fichier a sûrement été ouvert dans un éditeur de texte comme WordPad puis enregistré,
		//     ce qui a ajouté ces informations inutiles pour notre traitement ;
		//   - la ligne de header, contenant le nom de chaque feature ;
		//	 - les lignes vides.
		if (value.toString().length() > 0
				&& !value.toString().startsWith("\\")
				&& !value.toString().startsWith("{")
				&& !value.toString().startsWith("Region")) {
						
			// On récupère les valeurs de la ligne dans un tableau.
			String[] tokens = value.toString().split(",");
			
			// Profit total de cette ligne, en ignorant le dernier caractère ("\" ou "}").
			double totalProfit = Double.parseDouble(tokens[13].substring(0, tokens[13].length() - 1));
			
			// Si on doit obtenir le profit total d'une région et que la ligne est associée à la région donnée...
			if (taskName.equals("TOTAL_PROFIT_REGION") && tokens[0].equals(taskArg)) {
					context.write(new Text(taskArg), new DoubleWritable(totalProfit));
			}
			// Si on doit obtenir le profit total d'un pays et que la ligne est associée au pays donné...
			else if (taskName.equals("TOTAL_PROFIT_COUNTRY") && tokens[1].equals(taskArg)) {
				context.write(new Text(taskArg), new DoubleWritable(totalProfit));
			}
			// Si on doit obtenir le profit total d'un type de produit et que la ligne est associée au type de produit donné...
			else if (taskName.equals("TOTAL_PROFIT_ITEM_TYPE") && tokens[2].equals(taskArg)) {
				context.write(new Text(taskArg), new DoubleWritable(totalProfit));
			}
			// Si on doit obtenir le nombre d'unités vendues total par type de produit et de canal de vente (offline/online)...
			else if (taskName.equals("SALES_PER_ITEM_TYPE_AND_SALES_CHANNEL")) {
				context.write(new Text(tokens[2] + " (" + tokens[3] + ")"), new DoubleWritable(Double.parseDouble(tokens[8])));
			}
			// Si on doit obtenir le profit total par type de produit et de canal de vente (offline/online)...
			else if (taskName.equals("TOTAL_PROFIT_PER_ITEM_TYPE_AND_SALES_CHANNEL")) {
				context.write(new Text(tokens[2] + " (" + tokens[3] + ")"), new DoubleWritable(totalProfit));
			}
		}

	}
}