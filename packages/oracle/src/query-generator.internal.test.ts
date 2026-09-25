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

  it('uses Oracle VECTOR bindings in insert and update queries', () => {
    const attributes = {
      embedding: {
        attributeName: 'embedding',
        columnName: 'embedding',
        fieldName: 'embedding',
        type: sequelize.normalizeDataType(DataTypes.VECTOR(3)),
      },
    };
    const insert = queryGenerator.insertQuery('items', { embedding: [1, 2, 3] }, attributes);
    const update = queryGenerator.updateQuery(
      'items',
      { embedding: [4, 5, 6] },
      { id: 1 },
      {},
      attributes,
    );

    expect(insert.bind?.sequelize_1).to.deep.equal(Float32Array.from([1, 2, 3]));
    expect(update.bind?.sequelize_1).to.deep.equal(Float32Array.from([4, 5, 6]));
  });

  it('reads VECTOR_INFO when the connected database supports vectors', () => {
    sequelize.setDatabaseVersion('23.26.0');

    expect(queryGenerator.describeTableQuery('items')).to.include('atc.VECTOR_INFO');
  });

  it('does not read VECTOR_INFO when the database version is unknown or too old', () => {
    const unknownVersionSequelize = new Sequelize({ dialect: OracleDialect });
    const oldVersionSequelize = new Sequelize({ dialect: OracleDialect });
    oldVersionSequelize.setDatabaseVersion('21.3.0');

    expect(unknownVersionSequelize.queryGenerator.describeTableQuery('items')).not.to.include(
      'atc.VECTOR_INFO',
    );
    expect(oldVersionSequelize.queryGenerator.describeTableQuery('items')).not.to.include(
      'atc.VECTOR_INFO',
    );
  });
});
