package edu.buffalo.cse.sql.optimizer;

import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.plan.AggregateNode;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.plan.PlanNode.Type;

public class PushDownSelects extends PlanRewrite{

	protected PushDownSelects(boolean defaultTopDown) {
		super(defaultTopDown);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected PlanNode apply(PlanNode node) throws SqlException {
		// TODO Auto-generated method stub
		AggregateNode aggNode=null;
		if(node.type==Type.AGGREGATE){
			aggNode = (AggregateNode)node;	
			if(QueryOptimizer.isIndexScanNodeReplacement){
				aggNode.setChild(QueryOptimizer.iScanNode);
			}
			
		}
		//System.out.println(aggNode);
		/**remember that we are returning node and not aggNode**/
		return node;
	}
	
}