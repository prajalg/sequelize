import type { VectorElementType, VectorValue } from '../abstract-dialect/data-types.js';
import { VECTOR } from '../abstract-dialect/data-types.js';
import type { AbstractDialect } from '../abstract-dialect/dialect.js';
import type { EscapeOptions } from '../abstract-dialect/query-generator-typescript.js';
import type { ModelDefinition } from '../model-definition.js';
import type { ModelStatic } from '../model.js';
import { extractModelDefinition } from '../utils/model-utils.js';
import { AssociationPath } from './association-path.js';
import { Attribute } from './attribute.js';
import { BaseSqlExpression } from './base-sql-expression.js';
import { Cast } from './cast.js';
import { DialectAwareFn } from './dialect-aware-fn.js';
import { Value } from './value.js';

export type VectorMetric =
  'cosine' | 'euclidean' | 'euclideanSquared' | 'manhattan' | 'dot' | 'hamming' | 'jaccard';

const VECTOR_METRICS = new Set<VectorMetric>([
  'cosine',
  'euclidean',
  'euclideanSquared',
  'manhattan',
  'dot',
  'hamming',
  'jaccard',
]);

interface VectorDistanceInclude {
  as: string;
  subQuery?: boolean;
  separate?: boolean;
  include?: readonly VectorDistanceInclude[];
}

/** @internal Query scope supplied exclusively to vector-distance ORDER BY expressions. */
export interface VectorDistanceQueryContext {
  model?: ModelStatic | ModelDefinition | null;
  mainAlias?: string;
  derivedTable: boolean;
  isSubQuery?: boolean;
  attributes?: readonly unknown[];
  include?: readonly VectorDistanceInclude[];
  resolveAlias(tableAlias: string, attribute: string): string;
}

/**
 * Do not use me directly. Use `sql.vectorDistance` instead.
 */
export class VectorDistance extends DialectAwareFn {
  readonly metric: VectorMetric;

  constructor(
    left: BaseSqlExpression,
    right: BaseSqlExpression | VectorValue,
    metric: VectorMetric,
  ) {
    if (!(left instanceof BaseSqlExpression)) {
      throw new TypeError(
        'The left operand of sql.vectorDistance must be a SQL expression. Use sql.attribute(attributeName) to reference a model attribute.',
      );
    }

    if (!VECTOR_METRICS.has(metric)) {
      throw new TypeError(`Invalid vector distance metric: ${String(metric)}`);
    }

    super(left, right instanceof BaseSqlExpression ? right : new Value(right));
    this.metric = metric;
  }

  get maxArgCount() {
    return 2;
  }

  get minArgCount() {
    return 2;
  }

  /**
   * Returns whether the dialect supports this vector-distance metric.
   *
   * @param dialect The dialect to check.
   */
  supportsDialect(dialect: AbstractDialect): boolean {
    const support = dialect.supports.vectorDistance;

    return support !== false && support.metrics.includes(this.metric);
  }

  /**
   * Renders this vector-distance expression for the selected dialect.
   *
   * @param dialect The dialect that renders the expression.
   * @param options Options used while escaping the expression.
   */
  applyForDialect(dialect: AbstractDialect, options?: EscapeOptions): string {
    const context = options?.vectorDistanceContext;
    if (context) {
      options = {
        ...options,
        model: context.model,
        mainAlias: context.mainAlias,
        formatVectorOperand: attribute => formatOrderOperand(attribute, context, dialect, options),
      };
    }

    const left = this.args[0];
    const right = this.args[1];
    const leftSql = dialect.queryGenerator.escape(left, options);
    let rightSql: string;

    if (right instanceof Value) {
      const vectorType =
        getVectorDataType(left, options, dialect) ??
        inferVectorDataType(right.value as VectorValue);
      const bindParam = options?.bindParam ?? options?.vectorBindParam;
      rightSql = dialect.queryGenerator.escape(right.value, {
        ...options,
        type: vectorType,
        ...(bindParam ? { bindParam } : {}),
      });
    } else {
      rightSql = dialect.queryGenerator.escape(right, options);
    }

    return dialect.queryGenerator.formatVectorDistance(leftSql, rightSql, this.metric);
  }
}

function formatOrderOperand(
  attribute: Attribute | AssociationPath,
  context: VectorDistanceQueryContext,
  dialect: AbstractDialect,
  options: EscapeOptions | undefined,
): string {
  const generator = dialect.queryGenerator;
  const model = context.model ? extractModelDefinition(context.model) : null;
  const operandOptions = { ...options, formatVectorOperand: undefined };

  if (attribute instanceof AssociationPath) {
    let includes = context.include;
    for (const part of attribute.associationPath) {
      const include = includes?.find(item => item.as === part);
      if (!include || include.separate || (context.isSubQuery && !include.subQuery)) {
        throw new Error(
          `sql.vectorDistance cannot order by ${attribute.associationPath.join('.')}.${attribute.attributeName}: the association is not joined in this query scope.`,
        );
      }

      // An inner join is not visible outside the derived table. Do not guess its projection.
      if (context.derivedTable && !context.isSubQuery && include.subQuery) {
        throw new Error(
          'sql.vectorDistance cannot order by an association inside a pagination subquery. Select the distance with an alias and order by that alias instead.',
        );
      }

      includes = include.include;
    }

    return generator.escape(attribute, operandOptions);
  }

  const columnName = model?.getColumnNameLoose(attribute.attributeName) ?? attribute.attributeName;
  let outputName = columnName;
  if (context.derivedTable && !context.isSubQuery) {
    // A derived table exposes SELECT aliases, not necessarily the physical column names.
    const projection = context.attributes?.find(item => {
      const projected = Array.isArray(item) ? item[0] : item;
      const projectedColumnName =
        projected instanceof Attribute
          ? (model?.getColumnNameLoose(projected.attributeName) ?? projected.attributeName)
          : projected;

      return projectedColumnName === columnName;
    });
    if (projection === undefined && context.attributes && !context.attributes.includes('*')) {
      throw new Error(
        `sql.vectorDistance cannot order by ${attribute.attributeName}: the column is not selected by the pagination subquery. Include the attribute or select the distance with an alias.`,
      );
    }

    outputName = Array.isArray(projection) ? projection[1] : columnName;
    outputName = context.resolveAlias(context.mainAlias ?? '', outputName);
  }

  return generator.escape(new Attribute(outputName), { ...operandOptions, model: null });
}

/**
 * Builds a vector-distance SQL expression.
 *
 * @param left The vector column or SQL expression to search.
 * @param right A vector column, SQL expression, or literal query vector.
 * @param metric The distance metric to calculate.
 */
export function vectorDistance(
  left: BaseSqlExpression,
  right: BaseSqlExpression | VectorValue,
  metric: VectorMetric,
): VectorDistance {
  return new VectorDistance(left, right, metric);
}

function getVectorDataType(
  expression: unknown,
  options: EscapeOptions | undefined,
  dialect: AbstractDialect,
): VECTOR | undefined {
  const modelDefinition = options?.model ? extractModelDefinition(options.model) : null;
  const dataType = getExpressionDataType(expression, modelDefinition, dialect);

  return dataType instanceof VECTOR ? dataType : undefined;
}

function getExpressionDataType(
  expression: unknown,
  modelDefinition: ModelDefinition | null,
  dialect: AbstractDialect,
) {
  if (expression instanceof Cast && typeof expression.type !== 'string') {
    return dialect.sequelize.normalizeDataType(expression.type);
  }

  if (expression instanceof AssociationPath && modelDefinition) {
    const association = modelDefinition.getAssociation(expression.associationPath);

    return association?.target.modelDefinition.attributes.get(expression.attributeName)?.type;
  }

  if (expression instanceof Attribute && modelDefinition) {
    return modelDefinition.attributes.get(expression.attributeName)?.type;
  }

  return undefined;
}

function inferVectorDataType(value: VectorValue): VECTOR {
  let elementType: VectorElementType = 'float32';
  let dimensions = value.length;

  if (value instanceof Float64Array) {
    elementType = 'float64';
  } else if (value instanceof Int8Array) {
    elementType = 'int8';
  } else if (value instanceof Uint8Array) {
    elementType = 'binary';
    dimensions *= 8;
  }

  return new VECTOR({ dimensions, elementType });
}
