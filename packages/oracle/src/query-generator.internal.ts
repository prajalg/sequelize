// Copyright (c) 2025, Oracle and/or its affiliates. All rights reserved

import { attributeTypeToSql } from '@sequelize/core/_non-semver-use-at-your-own-risk_/abstract-dialect/data-types-utils.js';
import { AbstractQueryGeneratorInternal } from '@sequelize/core/_non-semver-use-at-your-own-risk_/abstract-dialect/query-generator-internal.js';
import type { EscapeOptions } from '@sequelize/core/_non-semver-use-at-your-own-risk_/abstract-dialect/query-generator-typescript.js';
import type { AddLimitOffsetOptions } from '@sequelize/core/_non-semver-use-at-your-own-risk_/abstract-dialect/query-generator.internal-types.js';
import { wrapAmbiguousWhere } from '@sequelize/core/_non-semver-use-at-your-own-risk_/abstract-dialect/where-sql-builder.js';
import type { Cast } from '@sequelize/core/_non-semver-use-at-your-own-risk_/expression-builders/cast.js';
import type { Fn } from '@sequelize/core/_non-semver-use-at-your-own-risk_/expression-builders/fn.js';
import util from 'node:util';
import type { OracleDialect } from './dialect.js';

const VECTOR_FUNCTIONS = new Set([
  'COSINE_DISTANCE',
  'INNER_PRODUCT',
  'L1_DISTANCE',
  'L2_DISTANCE',
  'VECTOR_DISTANCE',
]);

export class OracleQueryGeneratorInternal<
  Dialect extends OracleDialect = OracleDialect,
> extends AbstractQueryGeneratorInternal<Dialect> {
  addLimitAndOffset(options: AddLimitOffsetOptions) {
    let fragment = '';
    const offset = options.offset || 0;

    if (options.offset || options.limit) {
      fragment += ` OFFSET ${this.queryGenerator.escape(offset, options)} ROWS`;
    }

    if (options.limit) {
      fragment += ` FETCH NEXT ${this.queryGenerator.escape(options.limit, options)} ROWS ONLY`;
    }

    return fragment;
  }

  formatCast(cast: Cast, options?: EscapeOptions | undefined): string {
    const type = this.sequelize.normalizeDataType(cast.type);

    let castSql = wrapAmbiguousWhere(
      cast.expression,
      this.queryGenerator.escape(cast.expression, { ...options, type }),
    );
    const targetSql = attributeTypeToSql(type).toUpperCase();

    if (type === 'boolean') {
      castSql = `(CASE WHEN ${castSql}='true' THEN 1 ELSE 0 END)`;

      return `CAST(${castSql} AS NUMBER)`;
    } else if (type === 'TIMESTAMPTZ') {
      castSql = castSql.slice(0, -1);

      return `${castSql} RETURNING TIMESTAMP WITH TIME ZONE)`;
    }

    return `CAST(${castSql} AS ${targetSql})`;
  }

  formatFn(piece: Fn, options?: EscapeOptions): string {
    const fnName = piece.fn.toUpperCase();
    if (!VECTOR_FUNCTIONS.has(fnName)) {
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

    return `${fnName}(${columnSql}, ${this.#formatVectorArg(vectorArg, fnName)})`;
  }

  #formatVectorArg(arg: unknown, fnName: string): string {
    if (Array.isArray(arg)) {
      return this.#formatVectorFromIterable(arg);
    }

    if (ArrayBuffer.isView(arg) && !(arg instanceof DataView) && isNumericTypedArray(arg)) {
      return this.#formatVectorFromIterable(arg);
    }

    if (typeof arg === 'string') {
      const trimmed = arg.trim();
      if (!trimmed.toUpperCase().startsWith('VECTOR(')) {
        throw new Error(
          `${fnName} expects the second argument to be a vector array, typed array, or VECTOR literal string`,
        );
      }

      const vectorLiteralRegex =
        /^VECTOR\(\s*'\[\s*(?:[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?\s*(?:,\s*[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?\s*)*)?\]'\s*\)$/i;
      if (!vectorLiteralRegex.test(trimmed)) {
        throw new Error(`${fnName} expects a well-formed VECTOR literal string`);
      }

      return trimmed;
    }

    throw new Error(
      `${fnName} expects the second argument to be a vector array, typed array, or VECTOR literal string`,
    );
  }

  // Oracle expects VECTOR('[1,2,3]') literals. Reuse the iterable path for both plain arrays
  // and typed arrays so we only have one place that generates the comma-separated payload.
  #formatVectorFromIterable(values: Iterable<number>): string {
    const parts: number[] = [];
    for (const item of values) {
      if (typeof item !== 'number' || Number.isNaN(item)) {
        throw new Error(`${util.format('%O is not a valid vector element', item)}`);
      }

      parts.push(item);
    }

    return `VECTOR('[${parts.join(',')}]')`;
  }

  getAliasToken(): string {
    return '';
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
