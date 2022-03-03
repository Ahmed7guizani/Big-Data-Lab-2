package main;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;

// Note classe Driver (contient le main du programme Hadoop).
public class SalesAnalysis
{
	
	// Le main du programme.
	public static void main(String[] args) throws Exception
	{
		// Créé un object de configuration Hadoop.
		Configuration conf = new Configuration();
		// Permet à Hadoop de lire ses arguments génériques, récupère les arguments restants dans ourArgs.
		String[] ourArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (ourArgs.length < 3){
			System.out.println("At least 1 of the 4 following arguments have not been provided: <input_file> <output_directory> <task_name> <task_arg>");
			System.out.println("Usage: salesanalysis.jar <input_file> <output_directory> <task_name> <task_arg>");
			System.out.println("\t- <input_file>: absolute/relative path of the CSV file in hdfs ;");
			System.out.println("\t- <output_directory>: path of the output directory in hdfs, in which Hadoop will generate the output files and logs ;");
			System.out.println("\t- <task_name>: the type of analysis task to perform on the CSV file. Possible values and associated argument:");
			System.out.println("\t\t- TOTAL_PROFIT_REGION <region_name>: obtain the total profit for the given world region ;");
			System.out.println("\t\t- TOTAL_PROFIT_COUNTRY <country_name>: obtain the total profit for the given country ;");
			System.out.println("\t\t- TOTAL_PROFIT_ITEM_TYPE <item_type>: obtain the total profit for the given item type ;");
			System.out.println("\t\t- SALES_PER_ITEM_TYPE_AND_SALES_CHANNEL: obtain how many sales were performed per item type and sales channel (online/offline) ;");
			System.out.println("\t\t- TOTAL_PROFIT_PER_ITEM_TYPE_AND_SALES_CHANNEL: obtain the total profit per item type and sales channel (online/offline) ;");
			System.out.println("\t- <task_arg>: argument associated with the type of task to perform (<region_name>, <country_name> or <item_type>).");
			System.out.println("");
			System.out.println("Example:");
			System.out.println("\tsalesanalysis.jar /home/training/LAB2/input/sales.csv /home/training/LAB2/output/1 TOTAL_PROFIT_REGION ");
			System.exit(-1);
		}
		
		// Type de tâche à réaliser (3ème argument), 
		// et l'argument éventuel associé à cette tâche (4ème argument).
		// Types de tâche possibles et argumenté associé.
		//   - TOTAL_PROFIT_REGION <region_name>
		//   - TOTAL_PROFIT_COUNTRY <country_name>
		//   - TOTAL_PROFIT_ITEM_TYPE <item_type>
		//   - SALES_PER_ITEM_TYPE_AND_SALES_CHANNEL
		//   - TOTAL_PROFIT_PER_ITEM_TYPE_AND_SALES_CHANNEL
		// On enregistre cela dans la configuration, pour que ce soit accessible par le mapper et le reducer.
		conf.setStrings("taskName", ourArgs[2]);
		conf.setStrings("taskArg",  ourArgs.length < 4 ? "" : ourArgs[3]);
		
		// Obtient un nouvel objet Job: une tâche Hadoop. On fourni la configuration Hadoop ainsi qu'une description
		// textuelle de la tâche.
		Job job = Job.getInstance(conf, "SalesAnalysis");

		// Défini les classes Driver, Map et Reduce.
		job.setJarByClass(SalesAnalysis.class);
		job.setMapperClass(SalesAnalysisMap.class);
		job.setReducerClass(SalesAnalysisReduce.class);

		// Définit types cles/valeurs de notre programme Hadoop.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		// Définit les fichiers d'entrée du programme et le répertoire des résultats.
		// On se sert du premier et du deuxième argument restants pour permettre à l'utilisateur de les spécifier
		// lors de l'exécution.
		FileInputFormat.addInputPath(job, new Path(ourArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(ourArgs[1]));
		
		// On lance la tâche Hadoop. Si elle s'est effectuée correctement, on renvoie 0. Sinon, on renvoie -1.
		if(job.waitForCompletion(true))
			System.exit(0);
		System.exit(-1);
	}
}