import { DataTypes, Sequelize, ValidationErrorItem } from '@sequelize/core';
import * as BaseTypes from '@sequelize/core/_non-semver-use-at-your-own-risk_/abstract-dialect/data-types.js';
import { OracleDialect } from '@sequelize/oracle';
import { expect } from 'chai';
import { VECTOR } from './data-types-overrides';

describe('Oracle VECTOR data type', () => {
  const dialect = new Sequelize({ dialect: OracleDialect }).dialect;

  function toOracleVector(type: BaseTypes.VECTOR): VECTOR {
    return type.toDialectDataType(dialect) as VECTOR;
  }

  it('extends the core VECTOR type', () => {
    const type = new VECTOR(3);

    expect(type).to.be.instanceOf(VECTOR);
    expect(type).to.be.instanceOf(BaseTypes.VECTOR);
  });

  it('renders the default float32 element type explicitly', () => {
    expect(toOracleVector(DataTypes.VECTOR()).toSql()).to.equal('VECTOR(*, FLOAT32)');
    expect(toOracleVector(DataTypes.VECTOR(3)).toSql()).to.equal('VECTOR(3, FLOAT32)');
  });

  it('renders each Oracle-supported element type', () => {
    expect(
      toOracleVector(DataTypes.VECTOR({ dimensions: 3, elementType: 'float64' })).toSql(),
    ).to.equal('VECTOR(3, FLOAT64)');
    expect(
      toOracleVector(DataTypes.VECTOR({ dimensions: 3, elementType: 'int8' })).toSql(),
    ).to.equal('VECTOR(3, INT8)');
    expect(
      toOracleVector(DataTypes.VECTOR({ dimensions: 24, elementType: 'binary' })).toSql(),
    ).to.equal('VECTOR(24, BINARY)');
  });

  it('rejects element types unsupported by Oracle', () => {
    expect(() =>
      toOracleVector(DataTypes.VECTOR({ dimensions: 3, elementType: 'float16' })),
    ).to.throw('oracle does not support the VECTOR(float16) data type');
  });

  it('converts plain arrays to the matching Oracle driver typed array', () => {
    expect(toOracleVector(DataTypes.VECTOR(3)).toBindableValue([1, 2, 3])).to.be.instanceOf(
      Float32Array,
    );
    expect(
      toOracleVector(DataTypes.VECTOR({ dimensions: 3, elementType: 'float64' })).toBindableValue([
        1, 2, 3,
      ]),
    ).to.be.instanceOf(Float64Array);
    expect(
      toOracleVector(DataTypes.VECTOR({ dimensions: 3, elementType: 'int8' })).toBindableValue([
        1, 2, 3,
      ]),
    ).to.be.instanceOf(Int8Array);
  });

  it('keeps matching typed arrays unchanged', () => {
    const value = new Float32Array([1, 2, 3]);

    expect(toOracleVector(DataTypes.VECTOR(3)).toBindableValue(value)).to.equal(value);
  });

  it('validates values before binding them', () => {
    const type = toOracleVector(DataTypes.VECTOR(3));

    expect(() => type.toBindableValue([1, 2])).to.throw(ValidationErrorItem, 'but 3 were expected');
    expect(() => type.toBindableValue(new Float64Array([1, 2, 3]))).to.throw(
      ValidationErrorItem,
      'Float64Array is not valid for VECTOR element type float32',
    );
  });

  it('returns Oracle VECTOR bind metadata', () => {
    const type = toOracleVector(DataTypes.VECTOR(3));

    expect(type._getBindDef({ DB_TYPE_VECTOR: 'DB_TYPE_VECTOR' } as never)).to.deep.equal({
      type: 'DB_TYPE_VECTOR',
    });
  });
});
