import { DataTypes, Sequelize, sql } from '@sequelize/core';
import { OracleDialect } from '@sequelize/oracle';
import { expect } from 'chai';

describe('Oracle query generator internals', () => {
  const sequelize = new Sequelize({ dialect: OracleDialect });
  const queryGenerator = sequelize.queryGenerator;

  it('does not intercept raw VECTOR_DISTANCE function expressions', () => {
    const expression = sql.fn(
      'VECTOR_DISTANCE',
      sql.attribute('firstEmbedding'),
      sql.attribute('secondEmbedding'),
      sql.literal('COSINE'),
    );

    expect(queryGenerator.escape(expression)).to.equal(
      'VECTOR_DISTANCE("firstEmbedding", "secondEmbedding", COSINE)',
    );
  });

  it('preserves casts in raw vector function expressions', () => {
    const expression = sql.fn(
      'VECTOR_DISTANCE',
      sql.cast(sql.attribute('firstEmbedding'), DataTypes.VECTOR(3)),
      sql.attribute('secondEmbedding'),
      sql.literal('COSINE'),
    );

    expect(queryGenerator.escape(expression)).to.equal(
      'VECTOR_DISTANCE(CAST("firstEmbedding" AS VECTOR(3, FLOAT32)), "secondEmbedding", COSINE)',
    );
  });
});
