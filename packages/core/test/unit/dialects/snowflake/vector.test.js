'use strict';

const { expect } = require('chai');
const Support = require('../../../support');
const { Op, sql } = require('@sequelize/core');

const expectsql = Support.expectsql;
const current = Support.sequelize;
const queryGenerator = current.dialect.queryGenerator;

if (current.dialect.name === 'snowflake') {
  describe('[Snowflake Specific] VECTOR functions', () => {
    it('renders L2 distance from array input', () => {
      const where = sql.where(current.l2Distance('embedding', [1, 2, 3]), {
        [Op.lt]: 5,
      });

      expectsql(queryGenerator.whereItemsQuery(where), {
        snowflake: 'VECTOR_L2_DISTANCE("embedding", [1,2,3]::VECTOR(FLOAT, 3)) < 5',
      });
    });

    it('renders inner product from array input', () => {
      const where = sql.where(current.innerProduct('embedding', [1, 2, 3]), {
        [Op.gt]: 0,
      });

      expectsql(queryGenerator.whereItemsQuery(where), {
        snowflake: 'VECTOR_INNER_PRODUCT("embedding", [1,2,3]::VECTOR(FLOAT, 3)) > 0',
      });
    });

    it('renders documented cosine similarity function', () => {
      const where = sql.where(
        sql.fn('VECTOR_COSINE_SIMILARITY', sql.attribute('embedding'), [1, 2, 3]),
        Op.gt,
        0.5,
      );

      expectsql(queryGenerator.whereItemsQuery(where), {
        snowflake: 'VECTOR_COSINE_SIMILARITY("embedding", [1,2,3]::VECTOR(FLOAT, 3)) > 0.5',
      });
    });

    it('supports integer typed arrays', () => {
      const where = sql.where(current.l2Distance('embedding', new Int8Array([1, 2, 3])), {
        [Op.lt]: 10,
      });

      expectsql(queryGenerator.whereItemsQuery(where), {
        snowflake: 'VECTOR_L2_DISTANCE("embedding", [1,2,3]::VECTOR(INT, 3)) < 10',
      });
    });

    it('rejects cosineDistance because Snowflake documents cosine similarity instead', () => {
      expect(() =>
        queryGenerator.formatSqlExpression(current.cosineDistance('embedding', [1, 2, 3])),
      ).to.throw(Error, 'COSINE_DISTANCE is not implemented for the Snowflake vector sample');
    });

    it('rejects vectorDistance because Snowflake does not document a generic vector distance function', () => {
      expect(() =>
        queryGenerator.formatSqlExpression(current.vectorDistance('embedding', [1, 2, 3])),
      ).to.throw(Error, 'VECTOR_DISTANCE is not implemented for the Snowflake vector sample');
    });
  });
}
