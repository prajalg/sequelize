import type { VectorElementType, VectorValue } from '../abstract-dialect/data-types.js';
import { VECTOR } from '../abstract-dialect/data-types.js';
import type { AbstractDialect } from '../abstract-dialect/dialect.js';
import type { EscapeOptions } from '../abstract-dialect/query-generator-typescript.js';
import type { ModelDefinition } from '../model-definition.js';
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

/**
 * Do not use me directly. Use {@link @sequelize/core!sql.vectorDistance}.
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

  supportsDialect(dialect: AbstractDialect): boolean {
    const support = dialect.supports.vectorDistance;

    return support !== false && support.metrics.includes(this.metric);
  }

  applyForDialect(dialect: AbstractDialect, options?: EscapeOptions): string {
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
