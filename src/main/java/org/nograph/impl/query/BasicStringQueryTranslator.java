/* 

Copyright 2020 aholinch

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/
package org.nograph.impl.query;

import org.nograph.GraphQuery;
import org.nograph.GraphQuery.Criterion;
import org.nograph.GraphQuery.RangeCriterion;
import org.nograph.GraphQuery.SetCriterion;
import org.nograph.GraphQuery.SimpleCriterion;

public class BasicStringQueryTranslator implements QueryTranslator 
{
	protected String eqOperator = "=";
	
	public BasicStringQueryTranslator()
	{
		this("=");
	}
	
	public BasicStringQueryTranslator(String eq)
	{
		eqOperator = eq;
	}

	@Override
	public Object graphQueryToNativeNode(String prefix, GraphQuery query) {
		return queryToNative(prefix,query);
	}

	@Override
	public Object graphQueryToNativeRel(String prefix, GraphQuery query) {
		return queryToNative(prefix,query);
	}

	protected String queryToNative(String prefix, GraphQuery query)
	{
		return criterionToClause(prefix,query.getCriterion()).toString();
	}
	
	@Override
	public Object criterionToClause(String prefix, Criterion crit) {
		String qs = null;
		
		if(crit instanceof SimpleCriterion)
		{
			qs = simpleCriterionToClause(prefix,(SimpleCriterion)crit).toString();
		}
		else if(crit instanceof SetCriterion)
		{
			qs = setCriterionToClause(prefix,(SetCriterion)crit).toString();
		}
		else if(crit instanceof RangeCriterion)
		{
			qs = rangeCriterionToClause(prefix,(RangeCriterion)crit).toString();
		}
		
		return qs;
	}

	@Override
	public Object simpleCriterionToClause(String prefix, SimpleCriterion crit) 
	{
		String qs = null;
		
		String op = opToString(crit.getOperator());
		
		qs = crit.getKey() + op + valueToString(crit.getValue());
		
		return qs;
	}
	
	/**
	 * In case you want type-specific formatting
	 * @param val
	 * @return
	 */
	protected String valueToString(Object val)
	{
		return String.valueOf(val);
	}
	
	protected String opToString(int opCode)
	{
		String op = eqOperator;
		switch(opCode)
		{
			case SimpleCriterion.OP_EQUAL:
				op = eqOperator;
				break;
			case SimpleCriterion.OP_GT:
				op = ">";
				break;
			case SimpleCriterion.OP_LT:
				op = "<";
				break;
			case SimpleCriterion.OP_GE:
				op = ">=";
				break;
			case SimpleCriterion.OP_LE:
				op = "<=";
				break;
			case SimpleCriterion.OP_NOT_EQUAL:
				op = "!=";
				break;
			case SimpleCriterion.OP_LIKE:
				op = "LIKE";
				break;
		}
		
		return op;
	}

	@Override
	public Object setCriterionToClause(String prefix, SetCriterion crit) {
		String qs = null;
		
		int op = crit.getSetOperation();
		String opStr = " AND ";
		if(op == SetCriterion.COMB_OR)
		{
			opStr = " OR ";
		}
		
		int numcrit = crit.getNumCriteria();
		if(numcrit < 1)
		{
			return null;
		}
		
		Criterion tmpcrit = null;
		String tmp = null;
		
		tmpcrit = crit.getCriterion(0);
		tmp = criterionToClause(prefix,tmpcrit).toString();
		if(numcrit > 1)
		{
			qs = "("+tmp+")";
			for(int i=1; i<numcrit; i++)
			{
				tmpcrit = crit.getCriterion(i);
				tmp = criterionToClause(prefix,tmpcrit).toString();
				qs += opStr+"("+tmp+")";
			}
		}
		else
		{
			qs = tmp;
		}
		return qs;
	}

	@Override
	public Object rangeCriterionToClause(String prefix, RangeCriterion crit) {
		String valueStr1 = valueToString(crit.getMinValue());
		String valueStr2 = valueToString(crit.getMaxValue());
		
		String minB = "{";
		String maxB = "}";
		if(crit.getMinInclusive())
		{
			minB = "[";
		}
		if(crit.getMaxInclusive())
		{
			maxB = "]";
		}
		
		String qs = crit.getKey()+":"+minB + valueStr1 + " TO " + valueStr2 + maxB;
				
		return qs;
	}

}
