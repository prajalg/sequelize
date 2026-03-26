'use strict';

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
        snowflake: 'VECTOR_L2_DISTANCE("embedding", ARRAY_CONSTRUCT(1,2,3)::VECTOR(FLOAT, 3)) < 5',
      });
    });

    it('renders cosine distance using similarity mapping', () => {
      const where = sql.where(current.cosineDistance('embedding', [1, 2, 3]), {
        [Op.lt]: 0.5,
      });

      expectsql(queryGenerator.whereItemsQuery(where), {
        snowflake:
          '1 - VECTOR_COSINE_SIMILARITY("embedding", ARRAY_CONSTRUCT(1,2,3)::VECTOR(FLOAT, 3)) < 0.5',
      });
    });

    it('supports integer typed arrays', () => {
      const where = sql.where(current.l2Distance('embedding', new Int8Array([1, 2, 3])), {
        [Op.lt]: 10,
      });

      expectsql(queryGenerator.whereItemsQuery(where), {
        snowflake: 'VECTOR_L2_DISTANCE("embedding", ARRAY_CONSTRUCT(1,2,3)::VECTOR(INT, 3)) < 10',
      });
    });
  });
}
