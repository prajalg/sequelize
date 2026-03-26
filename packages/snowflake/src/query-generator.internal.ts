import { AbstractQueryGeneratorInternal } from '@sequelize/core/_non-semver-use-at-your-own-risk_/abstract-dialect/query-generator-internal.js';
import type { EscapeOptions } from '@sequelize/core/_non-semver-use-at-your-own-risk_/abstract-dialect/query-generator-typescript.js';
import type { AddLimitOffsetOptions } from '@sequelize/core/_non-semver-use-at-your-own-risk_/abstract-dialect/query-generator.internal-types.js';
import type { Fn } from '@sequelize/core/_non-semver-use-at-your-own-risk_/expression-builders/fn.js';
import util from 'node:util';
import type { SnowflakeDialect } from './dialect.js';

const TECHNICAL_SCHEMA_NAMES = Object.freeze([
  'INFORMATION_SCHEMA',
  'PERFORMANCE_SCHEMA',
  'SYS',
  'information_schema',
  'performance_schema',
  'sys',
]);

export class SnowflakeQueryGeneratorInternal<
  Dialect extends SnowflakeDialect = SnowflakeDialect,
> extends AbstractQueryGeneratorInternal<Dialect> {
  getTechnicalSchemaNames() {
    return TECHNICAL_SCHEMA_NAMES;
  }

  addLimitAndOffset(options: AddLimitOffsetOptions): string {
    let fragment = '';
    if (options.limit != null) {
      fragment += ` LIMIT ${this.queryGenerator.escape(options.limit, options)}`;
    } else if (options.offset) {
      fragment += ` LIMIT NULL`;
    }

    if (options.offset) {
      fragment += ` OFFSET ${this.queryGenerator.escape(options.offset, options)}`;
    }

    return fragment;
  }

  formatFn(piece: Fn, options?: EscapeOptions): string {
    const fnName = piece.fn.toUpperCase();
    if (!SNOWFLAKE_VECTOR_FUNCTION_MAP.has(fnName)) {
      return super.formatFn(piece, options);
    }

    if (piece.args.length !== 2) {
      throw new Error(`${fnName} expects exactly 2 arguments`);
    }

    const [columnArg, vectorArg] = piece.args;
    const columnSql =
      typeof columnArg === 'string'
        ? this.queryGenerator.quoteIdentifier(columnArg)
        : this.queryGenerator.escape(columnArg, options);
    const vectorSql = this.#formatVectorArg(vectorArg, fnName);

    const render = SNOWFLAKE_VECTOR_FUNCTION_MAP.get(fnName)!;

    return render(columnSql, vectorSql);
  }

  #formatVectorArg(arg: unknown, fnName: string): string {
    if (Array.isArray(arg)) {
      return this.#formatVectorFromArray(arg, 'FLOAT');
    }

    if (ArrayBuffer.isView(arg) && !(arg instanceof DataView) && isNumericTypedArray(arg)) {
      return this.#formatTypedArrayVector(arg);
    }

    if (typeof arg === 'string') {
      const trimmed = arg.trim();
      if (this.#looksLikeVectorLiteral(trimmed)) {
        return trimmed;
      }

      if (trimmed.startsWith('[') && trimmed.endsWith(']')) {
        try {
          const parsed = JSON.parse(trimmed);
          if (Array.isArray(parsed)) {
            return this.#formatVectorFromArray(parsed, 'FLOAT');
          }
        } catch {
          // fall through to error below
        }
      }
    }

    throw new Error(
      `${fnName} expects the second argument to be a number array, typed array, or VECTOR-compatible SQL literal`,
    );
  }

  // Snowflake users can still pass explicit SQL snippets (for instance reusing the result of TO_VECTOR);
  // keep the heuristics generous so we do not re-wrap values that already carry proper typing.
  #looksLikeVectorLiteral(literal: string): boolean {
    return (
      literal.toUpperCase().includes('::VECTOR') ||
      literal.startsWith('ARRAY_CONSTRUCT') ||
      literal.startsWith('(') ||
      literal.startsWith('1 - VECTOR_COSINE_SIMILARITY') ||
      literal.startsWith('VECTOR_')
    );
  }

  // Snowflake needs both an ARRAY_CONSTRUCT expression and an explicit VECTOR(...) cast,
  // which we synthesize here to spare API users from having to remember that syntax.
  #formatVectorFromArray(values: unknown[], elementType: 'FLOAT' | 'INT'): string {
    if (values.length === 0) {
      throw new Error('Vector arguments must contain at least one element');
    }

    const numericValues = values.map(value => {
      if (typeof value !== 'number' || Number.isNaN(value)) {
        throw new Error(util.format('%O is not a valid vector element', value));
      }

      return value;
    });

    return `ARRAY_CONSTRUCT(${numericValues.join(',')})::VECTOR(${elementType}, ${numericValues.length})`;
  }

  #formatTypedArrayVector(values: NumericTypedArray): string {
    const elementType = isIntegerTypedArray(values) ? 'INT' : 'FLOAT';

    return this.#formatVectorFromArray([...values], elementType);
  }
}

// Map the dialect-agnostic helper names exposed on Sequelize to the corresponding
// Snowflake functions. Any new helper can plug into this table without touching the formatter.
const SNOWFLAKE_VECTOR_FUNCTION_MAP = new Map<string, (column: string, vector: string) => string>([
  ['COSINE_DISTANCE', (column, vector) => `1 - VECTOR_COSINE_SIMILARITY(${column}, ${vector})`],
  ['INNER_PRODUCT', (column, vector) => `VECTOR_INNER_PRODUCT(${column}, ${vector})`],
  ['L1_DISTANCE', (column, vector) => `VECTOR_L1_DISTANCE(${column}, ${vector})`],
  ['L2_DISTANCE', (column, vector) => `VECTOR_L2_DISTANCE(${column}, ${vector})`],
  ['VECTOR_DISTANCE', (column, vector) => `VECTOR_L2_DISTANCE(${column}, ${vector})`],
]);

function isNumericTypedArray(value: ArrayBufferView): value is NumericTypedArray {
  return (
    value instanceof Int8Array ||
    value instanceof Uint8Array ||
    value instanceof Uint8ClampedArray ||
    value instanceof Int16Array ||
    value instanceof Uint16Array ||
    value instanceof Int32Array ||
    value instanceof Uint32Array ||
    value instanceof Float32Array ||
    value instanceof Float64Array
  );
}

type NumericTypedArray =
  | Int8Array
  | Uint8Array
  | Uint8ClampedArray
  | Int16Array
  | Uint16Array
  | Int32Array
  | Uint32Array
  | Float32Array
  | Float64Array;

function isIntegerTypedArray(
  value: NumericTypedArray,
): value is
  | Int8Array
  | Uint8Array
  | Uint8ClampedArray
  | Int16Array
  | Uint16Array
  | Int32Array
  | Uint32Array {
  return (
    value instanceof Int8Array ||
    value instanceof Uint8Array ||
    value instanceof Uint8ClampedArray ||
    value instanceof Int16Array ||
    value instanceof Uint16Array ||
    value instanceof Int32Array ||
    value instanceof Uint32Array
  );
}
