/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.connector.flink.table.source;

import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;


public class StarRocksExpressionExtractor implements ExpressionVisitor<String> {

    private static final Map<FunctionDefinition, Function<String[], String>> SUPPORT_FUNC = new HashMap<>();
        static {
            SUPPORT_FUNC.put(BuiltInFunctionDefinitions.EQUALS, args -> args[0] + " = " + args[1]);
            SUPPORT_FUNC.put(BuiltInFunctionDefinitions.NOT_EQUALS, args -> args[0] + " <> " + args[1]);
            SUPPORT_FUNC.put(BuiltInFunctionDefinitions.GREATER_THAN, args -> args[0] + " > " + args[1]);
            SUPPORT_FUNC.put(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, args -> args[0] + " >= " + args[1]);
            SUPPORT_FUNC.put(BuiltInFunctionDefinitions.LESS_THAN, args -> args[0] + " < " + args[1]);
            SUPPORT_FUNC.put(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, args -> args[0] + " <= " + args[1]);
            SUPPORT_FUNC.put(BuiltInFunctionDefinitions.AND, args -> args[0] + " and " + args[1]);
            SUPPORT_FUNC.put(BuiltInFunctionDefinitions.OR, args -> args[0] + " or " + args[1]);
            SUPPORT_FUNC.put(BuiltInFunctionDefinitions.IS_NULL, args -> args[0] + " is null");
            SUPPORT_FUNC.put(BuiltInFunctionDefinitions.IS_NOT_NULL, args -> args[0] + " is not null");
            SUPPORT_FUNC.put(BuiltInFunctionDefinitions.NOT, args -> args[0] + " = false");
        }

    @Override
    public String visit(CallExpression call) {
        FunctionDefinition funcDef = call.getFunctionDefinition();

        if (funcDef.equals(BuiltInFunctionDefinitions.CAST)) {
            return call.getChildren().get(0).accept(this);
        }

        if (SUPPORT_FUNC.containsKey(funcDef)) {

            List<String> operands = new ArrayList<>();
            for (Expression child : call.getChildren()) {
                String operand = child.accept(this);
                if (operand == null) {
                    return null;
                }
                operands.add(operand);
            }
            return "(" + SUPPORT_FUNC.get(funcDef).apply(operands.toArray(new String[0])) + ")";
        }
        return null;
    }

    @Override
    public String visit(ValueLiteralExpression valueLiteral) {
        LogicalTypeRoot typeRoot = valueLiteral.getOutputDataType().getLogicalType().getTypeRoot();
        if (typeRoot.equals(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) ||
            typeRoot.equals(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) ||
            typeRoot.equals(LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE) ||
            typeRoot.equals(LogicalTypeRoot.DATE)) {
            return "'" + valueLiteral.toString() + "'";
        }
        return valueLiteral.toString();
    }

    @Override
    public String visit(FieldReferenceExpression fieldReference) {
        return "`" + fieldReference.getName() + "`";
    }

    @Override
    public String visit(TypeLiteralExpression typeLiteral) {
        return typeLiteral.getOutputDataType().toString();
    }

    @Override
    public String visit(Expression other) {
        return null;
    }

}
