package edu.buffalo.cse.sql.interpret;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.cse.sql.data.Datum;

public class UnionInterpreter{
	
	public static boolean isUnion=false;
	public static List<Datum[]> lsUnion = new ArrayList<Datum[]>();
	public static List<Datum[]> lsUnionLeft = new ArrayList<Datum[]>();
	public static List<Datum[]> lsUnionRight = new ArrayList<Datum[]>();
	public static boolean isNullSourceLeft = false;
	public static boolean isNullSourceRight = false;
	public static int iSetOutputSize=0;
	public static int iSetInputSize = 0;
	public static List lsCol = new ArrayList();
	
	public static void interpreteUnion(){
		
		Iterator itr=lsCol.iterator();
		while(itr.hasNext()){
			System.out.println("IN UNION :: COL NAME = "+itr.next());
		}
		
		/**this indicates that the union of table is taken with itself, case 6**/
		if(isNullSourceLeft == false && isNullSourceRight == false && ((lsUnionLeft.size()==0)||(lsUnionRight.size()==0))){
			if(lsUnionLeft.size()==0)
				lsUnionLeft=lsUnionRight;
			if(lsUnionRight.size()==0)
				lsUnionRight=lsUnionLeft;
		}
		
		iSetInputSize = lsUnionLeft.get(0).length;
		if(isNullSourceRight ==true || isNullSourceLeft==true || (iSetInputSize==iSetOutputSize)){
			performUnion(lsUnionLeft, lsUnionRight);			
		}
		else
		if(iSetInputSize!=iSetOutputSize){
			performUnionDifferent(lsUnionLeft, lsUnionRight);
		}
		
		
		int i = lsUnionLeft.get(0).length;
		if(i==lsCol.size()){
			System.out.println("SIZE NNNN");
		}
		
	}
	
	static void performUnion(List<Datum[]> left,List<Datum[]> right){
		
		Iterator itrLeft =null;
		if(lsUnion.size()==0){
			itrLeft  = left.iterator();	
			if(!isNullSourceLeft){
				itrLeft.next();
				itrLeft.next();
			}
			while(itrLeft.hasNext()){
				Datum[] temp = (Datum[])itrLeft.next();
				lsUnion.add(temp);
			}
		}
		Iterator itrRight  = right.iterator();
		if(!isNullSourceRight){
			itrRight.next();
			itrRight.next();
		}
		while(itrRight.hasNext()){	
			Datum[] temp = (Datum[])itrRight.next();
			lsUnion.add(temp);
		}
		System.out.println("FINAL o/p = ");
		//RelationReader.readRelation(lsUnion);
	}
	
	
static void performUnionDifferent(List<Datum[]> left,List<Datum[]> right){	
		Iterator itrLeft =null;
		if(lsUnion.size()==0){
			itrLeft  = left.iterator();	
			if(!isNullSourceLeft){
				itrLeft.next();
				itrLeft.next();
			}
			
			Iterator it  = lsCol.iterator();
			List iLsCol=new ArrayList();
			while(it.hasNext()){
				int iColNum = ProjectionInterpreter.getColumnPosition(it.next().toString(),UnionInterpreter.lsUnionLeft,null);
				System.out.println(" Column ADDED = == ="+iColNum);
				iLsCol.add(iColNum);
			}
			
			int iCnt=0;
			while(itrLeft.hasNext()){
				Datum[] out = new Datum[iLsCol.size()];
				Datum[] temp = (Datum[])itrLeft.next();
				
				Iterator ipos = iLsCol.iterator();
				while(ipos.hasNext()){
				 int i = Integer.parseInt(ipos.next().toString());
				 out[iCnt] =temp[i];
				 iCnt++;
				}
			
				iCnt=0;
				lsUnion.add(out);
			}
			
		}
		
		Iterator itrRight  = right.iterator();
		if(!isNullSourceRight){
			itrRight.next();
			itrRight.next();
		}
		
		Iterator it  = lsCol.iterator();
		List iLsCol=new ArrayList();
		while(it.hasNext()){
			int iColNum = ProjectionInterpreter.getColumnPosition(it.next().toString(),UnionInterpreter.lsUnionRight,null);
			iLsCol.add(iColNum);
		}
		
		int iCnt=0;
		while(itrRight.hasNext()){
			Datum[] out = new Datum[iLsCol.size()];
			Datum[] temp = (Datum[])itrRight.next();
			
			Iterator ipos = iLsCol.iterator();
			while(ipos.hasNext()){
			 int i = Integer.parseInt(ipos.next().toString());
			 out[iCnt] =temp[i]; 
			 iCnt++;
			}
			iCnt=0;
			lsUnion.add(out);
		}
	//	System.out.println("FINAL o/p of union = ");
	//	RelationReader.readRelation(lsUnion);
	}
}