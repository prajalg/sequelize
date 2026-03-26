'use strict';

const Support = require('../../../support');
const { Op, sql } = require('@sequelize/core');

const expectsql = Support.expectsql;
const current = Support.sequelize;
const queryGenerator = current.dialect.queryGenerator;

if (current.dialect.name === 'postgres') {
  describe('[Postgres Specific] VECTOR functions', () => {
    it('renders l2 distance using pgvector literal', () => {
      const where = sql.where(current.l2Distance('embedding', [1, 2, 3]), {
        [Op.lt]: 1.5,
      });

      expectsql(queryGenerator.whereItemsQuery(where), {
        postgres: `vector_l2_distance("embedding", '[1,2,3]'::vector) < 1.5`,
      });
    });

    it('renders cosine distance', () => {
      const where = sql.where(current.cosineDistance('embedding', [1, 2, 3]), {
        [Op.lt]: 0.5,
      });

      expectsql(queryGenerator.whereItemsQuery(where), {
        postgres: `vector_cosine_distance("embedding", '[1,2,3]'::vector) < 0.5`,
      });
    });

    it('supports typed arrays', () => {
      const where = sql.where(current.innerProduct('embedding', new Float32Array([1, 2, 3])), {
        [Op.gt]: 0,
      });

      expectsql(queryGenerator.whereItemsQuery(where), {
        postgres: `vector_inner_product("embedding", '[1,2,3]'::vector) > 0`,
      });
    });
  });
}
