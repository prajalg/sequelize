import { AbstractQueryGeneratorInternal } from '@sequelize/core/_non-semver-use-at-your-own-risk_/abstract-dialect/query-generator-internal.js';
import type { EscapeOptions } from '@sequelize/core/_non-semver-use-at-your-own-risk_/abstract-dialect/query-generator-typescript.js';
import type { AddLimitOffsetOptions } from '@sequelize/core/_non-semver-use-at-your-own-risk_/abstract-dialect/query-generator.internal-types.js';
import type { Fn } from '@sequelize/core/_non-semver-use-at-your-own-risk_/expression-builders/fn.js';
import util from 'node:util';
import type { PostgresDialect } from './dialect.js';

const TECHNICAL_DATABASE_NAMES = Object.freeze(['postgres']);
const TECHNICAL_SCHEMA_NAMES = Object.freeze([
  'information_schema',
  'tiger',
  'tiger_data',
  'topology',
]);

export class PostgresQueryGeneratorInternal<
  Dialect extends PostgresDialect = PostgresDialect,
> extends AbstractQueryGeneratorInternal<Dialect> {
  getTechnicalDatabaseNames() {
    return TECHNICAL_DATABASE_NAMES;
  }

  getTechnicalSchemaNames() {
    return TECHNICAL_SCHEMA_NAMES;
  }

  addLimitAndOffset(options: AddLimitOffsetOptions): string {
    let fragment = '';
    if (options.limit != null) {
      fragment += ` LIMIT ${this.queryGenerator.escape(options.limit, options)}`;
    }

    if (options.offset) {
      fragment += ` OFFSET ${this.queryGenerator.escape(options.offset, options)}`;
    }

    return fragment;
  }

  formatFn(piece: Fn, options?: EscapeOptions): string {
    const fnName = piece.fn.toUpperCase();
    const mappedName = POSTGRES_VECTOR_FUNCTION_MAP.get(fnName);
    if (!mappedName) {
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

    return `${mappedName}(${columnSql}, ${vectorSql})`;
  }

  #formatVectorArg(arg: unknown, fnName: string): string {
    if (Array.isArray(arg)) {
      return this.#formatVectorFromArray(arg);
    }

    if (ArrayBuffer.isView(arg) && !(arg instanceof DataView) && isNumericTypedArray(arg)) {
      return this.#formatVectorFromArray([...arg]);
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
            return this.#formatVectorFromArray(parsed);
          }
        } catch {
          // ignore and fall through
        }
      }
    }

    throw new Error(
      `${fnName} expects the second argument to be a number array, typed array, or PostgreSQL vector literal`,
    );
  }

  // Allow callers to hand-roll literals (e.g. `'[0,1,2]'::vector`) without Sequelize rewriting them.
  #looksLikeVectorLiteral(literal: string): boolean {
    return literal.endsWith('::vector') || literal.toLowerCase().startsWith('vector(');
  }

  // pgvector accepts JSON-ish string literals cast to vector; build one from plain JS arrays.
  #formatVectorFromArray(values: unknown[]): string {
    if (values.length === 0) {
      throw new Error('Vector arguments must contain at least one element');
    }

    const numericValues = values.map(value => {
      if (typeof value !== 'number' || Number.isNaN(value)) {
        throw new Error(util.format('%O is not a valid vector element', value));
      }

      return value;
    });

    return `'[${numericValues.join(',')}]'::vector`;
  }
}

function isNumericTypedArray(
  value: ArrayBufferView,
): value is
  | Int8Array
  | Uint8Array
  | Uint8ClampedArray
  | Int16Array
  | Uint16Array
  | Int32Array
  | Uint32Array
  | Float32Array
  | Float64Array {
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

// Sample cross-dialect implementation: pgvector can map the generic Sequelize helper names
// onto its own function surface without changing the shared API.
const POSTGRES_VECTOR_FUNCTION_MAP = new Map<string, string>([
  ['COSINE_DISTANCE', 'vector_cosine_distance'],
  ['INNER_PRODUCT', 'vector_inner_product'],
  ['L1_DISTANCE', 'vector_l1_distance'],
  ['L2_DISTANCE', 'vector_l2_distance'],
  ['VECTOR_DISTANCE', 'vector_l2_distance'],
]);
