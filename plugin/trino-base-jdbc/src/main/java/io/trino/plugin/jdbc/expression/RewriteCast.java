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
package io.trino.plugin.jdbc.expression;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.expression;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static java.lang.String.format;

public class RewriteCast
        implements ConnectorExpressionRule<Call, ParameterizedExpression>
{
    private static final Capture<ConnectorExpression> VALUE = newCapture();
    private static final Capture<List<ConnectorExpression>> EXPRESSIONS = newCapture();

    @Override
    public Pattern<Call> getPattern()
    {
        return call()
                .with(functionName().equalTo(CAST_FUNCTION_NAME))
                .with(argument(0).matching(expression().capturedAs(VALUE)));
    }

    @Override
    public Optional<ParameterizedExpression> rewrite(Call call, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        Type targetType = call.getType();
        ConnectorExpression capturedValue = captures.get(VALUE);

        Optional<ParameterizedExpression> value = context.defaultRewrite(capturedValue);
        if (value.isEmpty()) {
            return Optional.empty();
        }

        ImmutableList.Builder<QueryParameter> parameters = ImmutableList.builder();
        Optional<ParameterizedExpression> rewritten = context.defaultRewrite(capturedValue);
        if (rewritten.isEmpty()) {
            // if argument is a call chain that can't be rewritten, then we can't push it down
            return Optional.empty();
        }

        return Optional.of(new ParameterizedExpression(
                format("CAST(%s AS %s)", value.get().expression(), targetType.getDisplayName()),
                parameters.build()));
    }
}
