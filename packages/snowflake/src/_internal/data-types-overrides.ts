import type { AbstractDialect } from '@sequelize/core';
import { ValidationErrorItem } from '@sequelize/core';
import type { AcceptedDate } from '@sequelize/core/_non-semver-use-at-your-own-risk_/abstract-dialect/data-types.js';
import * as BaseTypes from '@sequelize/core/_non-semver-use-at-your-own-risk_/abstract-dialect/data-types.js';
import maxBy from 'lodash/maxBy.js';
import util from 'node:util';

export class DATE extends BaseTypes.DATE {
  toSql() {
    return `TIMESTAMP${this.options.precision != null ? `(${this.options.precision})` : ''}`;
  }

  toBindableValue(date: AcceptedDate) {
    date = this._applyTimezone(date);

    return date.format('YYYY-MM-DD HH:mm:ss.SSS');
  }
}

export class UUID extends BaseTypes.UUID {
  toSql() {
    // https://community.snowflake.com/s/question/0D50Z00009LH2fl/what-is-the-best-way-to-store-uuids
    return 'VARCHAR(36)';
  }
}

export class ENUM<Member extends string> extends BaseTypes.ENUM<Member> {
  toSql() {
    const minLength = maxBy(this.options.values, value => value.length)?.length ?? 0;

    // db2 does not have an ENUM type, we use VARCHAR instead.
    return `VARCHAR(${Math.max(minLength, 255)})`;
  }
}

export class TEXT extends BaseTypes.TEXT {
  toSql() {
    return 'TEXT';
  }
}

/** @deprecated */
export class REAL extends BaseTypes.REAL {
  toSql(): string {
    return 'REAL';
  }
}

export class FLOAT extends BaseTypes.FLOAT {
  // TODO: warn that FLOAT is not supported in Snowflake, only DOUBLE is

  toSql(): string {
    return 'FLOAT';
  }
}

export class DOUBLE extends BaseTypes.DOUBLE {
  toSql(): string {
    // FLOAT is a double-precision floating point in Snowflake
    return 'FLOAT';
  }
}

// Snowflake only has one int type: Integer, which is -99999999999999999999999999999999999999 to 99999999999999999999999999999999999999
export class TINYINT extends BaseTypes.TINYINT {
  toSql() {
    return 'INTEGER';
  }
}

export class SMALLINT extends BaseTypes.SMALLINT {
  toSql() {
    return 'INTEGER';
  }
}

export class MEDIUMINT extends BaseTypes.MEDIUMINT {
  toSql() {
    return 'INTEGER';
  }
}

export class INTEGER extends BaseTypes.INTEGER {
  toSql() {
    return 'INTEGER';
  }
}

export class BIGINT extends BaseTypes.BIGINT {
  // not really true, but snowflake allows INT values up to 99999999999999999999999999999999999999,
  // which is more than enough to cover a 64-bit unsigned integer (0 - 18446744073709551615)
  protected _supportsNativeUnsigned(_dialect: AbstractDialect): boolean {
    return true;
  }

  toSql() {
    return 'INTEGER';
  }
}

// Snowflake accepts VECTOR elements either as FLOAT or INT; normalize the various
// aliases so downstream render & validation logic only has to branch on two cases.
const SNOWFLAKE_FLOAT_FORMATS = new Set(['float', 'float32', 'float64', 'double']);
const SNOWFLAKE_INT_FORMATS = new Set(['int', 'int8', 'int16', 'int32', 'int64', 'integer']);

export class VECTOR extends BaseTypes.VECTOR {
  readonly #elementType: 'FLOAT' | 'INT';

  constructor(...args: ConstructorParameters<typeof BaseTypes.VECTOR>) {
    super(...args);

    const normalizedFormat = this.#normalizeFormat(this.options.format);
    this.#elementType = normalizedFormat === 'int' ? 'INT' : 'FLOAT';
    this.options.format = normalizedFormat;

    // Snowflake requires the dimension to be provided up front; surface a clearer
    // error here rather than letting the server reject the DDL later.
    if (this.options.dimension == null || !Number.isInteger(this.options.dimension)) {
      throw new TypeError('Snowflake VECTOR requires a positive integer "dimension" option.');
    }

    if (this.options.dimension <= 0) {
      throw new TypeError(
        'Snowflake VECTOR requires the "dimension" option to be greater than zero.',
      );
    }
  }

  protected getSqlOptionParts(): string[] {
    return [this.#elementType, String(this.options.dimension)];
  }

  validate(value: unknown): asserts value is BaseTypes.Vector {
    super.validate(value);

    const length = this.#getVectorLength(value);
    if (length !== this.options.dimension) {
      ValidationErrorItem.throwDataTypeValidationError(
        util.format(
          'VECTOR expects values of length %d, but received %d',
          this.options.dimension,
          length,
        ),
      );
    }
  }

  #normalizeFormat(format: string | undefined): 'float' | 'int' {
    if (format == null) {
      return 'float';
    }

    const lower = format.toLowerCase();
    if (SNOWFLAKE_FLOAT_FORMATS.has(lower)) {
      return 'float';
    }

    if (SNOWFLAKE_INT_FORMATS.has(lower)) {
      return 'int';
    }

    throw new TypeError(
      `Snowflake VECTOR format "${format}" is not supported. Use "float" (default) or "int".`,
    );
  }

  #getVectorLength(value: BaseTypes.Vector): number {
    if (Array.isArray(value)) {
      return value.length;
    }

    if (
      value instanceof Int8Array ||
      value instanceof Uint8Array ||
      value instanceof Uint8ClampedArray ||
      value instanceof Int16Array ||
      value instanceof Uint16Array ||
      value instanceof Int32Array ||
      value instanceof Uint32Array ||
      value instanceof Float32Array ||
      value instanceof Float64Array ||
      value instanceof BigInt64Array ||
      value instanceof BigUint64Array
    ) {
      return value.length;
    }

    throw new TypeError('Unsupported vector container type');
  }

  protected _validateVectorElement(item: unknown): number {
    const numeric = super._validateVectorElement(item);

    if (this.options.format === 'int' && !Number.isInteger(numeric)) {
      ValidationErrorItem.throwDataTypeValidationError(
        util.format(
          'VECTOR(INT, %d) only accepts integers, but received %O',
          this.options.dimension,
          numeric,
        ),
      );
    }

    return numeric;
  }
}
