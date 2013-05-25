package edu.buffalo.cse.sql.interpret;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import edu.buffalo.cse.sql.data.Datum;

public class OrderByInterpreter {
	
	
	public static void interpreteOrderBy(List<Datum[]> datumList, int c1, int c2, String t1, String t2)
	{
		
		Set<String> set1 = new TreeSet<String>();
		Set<String> set1_d = new TreeSet<String>(Collections.reverseOrder());
		
		
	    List<Datum[]> datumList2 = null;
	    
	if(c2==-1)
	{
		if(t1.equalsIgnoreCase(null))
		{
			for (int i = 0; i < datumList.size(); i++) 
			{
			       set1.add(datumList.get(i)[c1].toString());
			}
			
			for (String key : set1) 
			{
				 for(int i=0;i<datumList.size();i++)
				 {	//We may have to create indices for this
				
					 if(key.equalsIgnoreCase(datumList.get(i)[c1].toString()))
					 {
						 	datumList2.add(datumList.get(i));
					 }

				 }
			} 
		}
		else if(t1.equalsIgnoreCase("DESC"))
		{

			//We can use this to get the set in reverse order and continue the previous step
			/*
			TreeSet<Integer> treeSetObj = new TreeSet<Integer>( Collections.reverseOrder() ) ;
			 */
			for (int i = 0; i < datumList.size(); i++) 
			{
			       set1_d.add(datumList.get(i)[c1].toString());
			       
			}
			for (String key : set1) 
			{
				for(int i=0;i<datumList.size();i++)
				{	//We may have to create indices for this
				
					if(key.equalsIgnoreCase(datumList.get(i)[c1].toString()))
					{
							datumList2.add(datumList.get(i));
					}
				}
			}


		}
	}

	else {


		Set<String> set2 = new TreeSet<String>();
		Set<String> set2_d = new TreeSet<String>();
		
		List<Datum[]> datumList3 = null;
		if(t1.equalsIgnoreCase("DESC"))
		{
			for (int i = 0; i < datumList.size(); i++) 
			{
			       set1_d.add(datumList.get(i)[c1].toString());
			       
			}
			for (String key : set1_d) 
			{
				for(int i=0;i<datumList.size();i++)
				{	

					if(key.equalsIgnoreCase(datumList.get(i)[c1].toString()))
					{
						datumList2.add(datumList.get(i));
					}

				}
			}

			for (int i = 0; i < datumList2.size(); i++) 
			{
			       set2.add(datumList.get(i)[c2].toString());
			       
			}
			for (String key2 : set2) 
			{
				
				for(int i=0;i<datumList2.size();i++)
				{	//We may have to create indices for this
					if(key2.equalsIgnoreCase(datumList2.get(i)[c2].toString()))
					{
						datumList3.add(datumList.get(i));
					}

				}
			}
		}

		else
		{
			for (int i = 0; i < datumList.size(); i++) 
			{
			       set1.add(datumList.get(i)[c1].toString());
			}
			for (String key : set1) 
			{
				for(int i=0;i<datumList.size();i++)
				{	//We may have to create indices for this
					if(key.equalsIgnoreCase(datumList.get(i)[c1].toString()))
					{
							datumList2.add(datumList.get(i));
					}

				}
			}

			for (int i = 0; i < datumList2.size(); i++) 
			{
			       set2.add(datumList.get(i)[c2].toString());
			}
			for (String key2 : set2) 
			{
				for(int i=0;i<datumList2.size();i++)
				{	
						if(key2.equalsIgnoreCase(datumList2.get(i)[c2].toString()))
						{
							datumList3.add(datumList.get(i));
						}

				}
			}


	}

	}

}
	
}

