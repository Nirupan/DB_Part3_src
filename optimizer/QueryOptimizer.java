package edu.buffalo.cse.sql.optimizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.interpret.IndexScanInterpreter;
import edu.buffalo.cse.sql.interpret.UnionInterpreter;
import edu.buffalo.cse.sql.plan.*;
import edu.buffalo.cse.sql.plan.PlanNode.Structure;
import edu.buffalo.cse.sql.plan.PlanNode.Type;

public class QueryOptimizer{
	
	public static Stack PRStack=new Stack();
	public static boolean isIndexScanNodeReplacement=false;
	public static IndexScanNode iScanNode=null;
	public static List<ExprTree> lsHHJMember=new ArrayList<ExprTree>();
	
	public static PlanNode optimizeQuery(PlanNode q){
		pushPlanNode(q);
		/**to decide where to perform optimization**/
		PlanNode rewrittenPN = popQuery();
		return rewrittenPN;
	}

	public static void pushPlanNode(PlanNode node)
	{
	//System.out.println("INSIDE pushPlanNode");
	if (node.type == Type.AGGREGATE) 
		{
		//System.out.println("Type  = " + node.type);
		AggregateNode aggregate = (AggregateNode) node;
		PRStack.push(aggregate);		
		//System.out.println("  -> " + aggregate.getAggregates());
		PlanNode aggrChild = aggregate.getChild();
		pushPlanNode(aggrChild);

		}
	if (node.type == Type.SELECT) {
		//System.out.println("Type =  " + node.type);
		SelectionNode selectChild = (SelectionNode) node;
		PRStack.push(selectChild);
		//System.out.println("  -> " + selectChild.getCondition());
		PlanNode sltChild = selectChild.getChild();
		pushPlanNode(sltChild);
	}
	
	if (node.type == Type.JOIN) {
		//System.out.println("Type =  " + node.type);
		JoinNode joinNode = (JoinNode) node;
		PlanNode rightChild = joinNode.getRHS();
		PlanNode leftChild = joinNode.getLHS();
	/**first pushed children*/
		pushPlanNode(leftChild);
		pushPlanNode(rightChild);
	
	//	PRStack.push(joinNode);		
		//System.out.println("join type = " + joinNode.getJoinType());
		//System.out.println("   ->" + joinNode.getLHS());
		//System.out.println("   ->" + joinNode.getRHS());
		
	//	pushPlanNode(leftChild);
	//	pushPlanNode(rightChild);
	}
	if (node.type == Type.NULLSOURCE) {
		//System.out.println("Type =  " + node.type);
		NullSourceNode nullSrcNode = (NullSourceNode) node;
		PRStack.push(nullSrcNode);
	}
	if (node.type == Type.PROJECT) {
		//System.out.println("Type =  " + node.type);
		ProjectionNode projectionNode = (ProjectionNode) node;
		PRStack.push(projectionNode);
		node = projectionNode.getChild();
		//System.out.println("Type =  " + node.type);
		pushPlanNode(node);
	}
	if (node.type == Type.SCAN) {
		//System.out.println("Type =  " + node.type);
		ScanNode scanNode = (ScanNode) node;
		PRStack.push(scanNode);
		//System.out.println("   ->" + scanNode.table);
	}
	if (node.type == Type.UNION) {
		//System.out.println("child =   " + node.type);
		UnionNode unionNode = (UnionNode) node;
		PRStack.push(unionNode);
		UnionInterpreter.isUnion=true;
		PlanNode leftChild = unionNode.getLHS();
		PlanNode rightChild = unionNode.getRHS();
		pushPlanNode(leftChild);
		pushPlanNode(rightChild);
	}
}
	
	public static PlanNode  interpreteExprTree(ExprTree exprTree){
//		//System.out.println(exprTree);
		if(exprTree.op==ExprTree.OpCode.EQ){
			lsHHJMember.add(exprTree);
			
			
		}
		if(exprTree.op==ExprTree.OpCode.GT || exprTree.op==ExprTree.OpCode.GTE || exprTree.op==ExprTree.OpCode.LT || exprTree.op==ExprTree.OpCode.LTE){
			//System.out.println(exprTree);
			iScanNode=new IndexScanNode(Structure.LEAF,Type.SCAN);
			//System.out.println("0 :: "+exprTree.get(0));
			//System.out.println("1 :: "+exprTree.get(1));
		}
		for(int i=0;i<exprTree.size();i++){
			ExprTree expr=exprTree.get(i);
			interpreteExprTree(expr);
		}
		
		return null;
	}

	public static PlanNode popQuery()
	{
		//System.out.println("INSIDE popQuery");
		PlanNode planNode=null;
		PlanNode prevNode=null;
		boolean isPrevPop_SCAN = false;
		boolean isPrevPop_JOIN = false;
		IndexScanInterpreter i=new IndexScanInterpreter();
		int j;int k=1;
		j=k++;
		iScanNode=new IndexScanNode(Structure.LEAF,Type.SCAN);
		while (!PRStack.isEmpty()) {
			
			////System.out.println("PlanNode Query is:="+PRStack);
			planNode = (PlanNode) PRStack.pop();
		//	//System.out.println("Popped "+planNode.type);
			//System.out.println("popped type = " + planNode.type);
			if (planNode.type == Type.SELECT) {
				if (isPrevPop_SCAN) {
					//System.out.println("PATH :: isIndexScanNodeReplacement=true");
					isIndexScanNodeReplacement=true;
					
					SelectionNode selectNode = (SelectionNode)planNode;
					
					if(selectNode.getChild().type == Type.JOIN){
						//System.out.println("SELECT CONDITION ---- "+selectNode.getChild().type);
						if(selectNode.conjunctiveClauses().toString().contains("=")){
							//System.out.println("CONTAINS == ");
							ExprTree e = selectNode.getCondition();
						//	//System.out.println(e.get(0).get(0).get(0).get(0).get(0).get(0).get(0).get(0).get(0).get(0).op);
						//	//System.out.println(e.get(1));

						}

					}
					//System.out.println("SELECT CONDITION ---- "+selectNode.conjunctiveClauses());

					iScanNode.condition=selectNode.getCondition();
					ScanNode scanNode=(ScanNode)prevNode;
					//System.out.println("TABLE SIZE :: "+scanNode.detailString());
					//iScanNode.table[0]= scanNode.table;
					//iScanNode.schema[0]= scanNode.schema;
					iScanNode.table.add(scanNode.table);
					iScanNode.schema.add(scanNode.schema);
					
				}
				if(isPrevPop_JOIN){
					SelectionNode selectNode = (SelectionNode)planNode;
						ExprTree exprTree = selectNode.getCondition();
						interpreteExprTree(exprTree);							
				}
					
						
			}

			isPrevPop_SCAN = false;
			if (planNode.type == Type.SCAN) {
				isPrevPop_SCAN = true;
				ScanNode sn = (ScanNode)planNode;
				iScanNode.table.add(sn.table);
				iScanNode.schema.add(sn.schema);
				
				//System.out.println("--"+sn.detailString());
			}
			
			if(planNode.type == Type.JOIN){
				JoinNode jn = (JoinNode)planNode;
				//System.out.println("--"+jn.getLHS());
				if(isPrevPop_SCAN){
					isPrevPop_SCAN=false;
				}
				isPrevPop_JOIN = true;
			}
			
			if (planNode.type == Type.AGGREGATE) {
				if (isIndexScanNodeReplacement) {
					//System.out.println("PATH :: replaceSELECTSCAN");
					PushDownSelects replaceSELECTSCAN = new PushDownSelects(true);
					AggregateNode aggNode = (AggregateNode)planNode;
					try {
						PlanNode rewrittenPN = replaceSELECTSCAN.rewrite(aggNode);
					//	//System.out.println("PATH :: rewrittenPN = \n"+rewrittenPN);
						return rewrittenPN;
					} catch (SqlException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}	 
			
			prevNode=planNode; 
		}
	return null;		
}

}