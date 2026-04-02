'use strict';

const { DataTypes, Op, sql } = require('@sequelize/core');
const { expect } = require('chai');
const { getTestDialect, sequelize } = require('../../../support');

if (getTestDialect() === 'snowflake') {
  describe('[Snowflake Specific] vectors', () => {
    describe('FLOAT vectors', () => {
      beforeEach(async function () {
        this.Item = sequelize.define(
          'SnowflakeVectorItem',
          {
            id: {
              type: DataTypes.INTEGER,
              primaryKey: true,
            },
            embeddings: DataTypes.VECTOR(3),
          },
          {
            tableName: 'SnowflakeVectorItems',
            freezeTableName: true,
            timestamps: false,
          },
        );

        await this.Item.sync({ force: true });
        await sequelize.query(`
          INSERT INTO "SnowflakeVectorItems" ("id", "embeddings")
          VALUES
            (1, [1,1,1]::VECTOR(FLOAT, 3)),
            (2, [5,5,5]::VECTOR(FLOAT, 3)),
            (3, [10,10,10]::VECTOR(FLOAT, 3)),
            (4, [1,2,3]::VECTOR(FLOAT, 3))
        `);
      });

      it('fetches rows from a vector column', async function () {
        const result = await this.Item.findAll({
          order: [['id', 'ASC']],
        });

        expect(result).to.have.length(4);
      });

      it('supports l2 distance filtering through helper methods', async function () {
        const result = await this.Item.findAll({
          attributes: ['id'],
          where: sql.where(sequelize.l2Distance('embeddings', [1, 2, 3]), Op.lt, 3),
          order: [['id', 'ASC']],
        });

        expect(result.map(row => row.get('id'))).to.deep.equal([1, 4]);
      });

      it('supports inner product filtering through helper methods', async function () {
        const result = await this.Item.findAll({
          attributes: ['id'],
          where: sql.where(sequelize.innerProduct('embeddings', [1, 2, 3]), Op.gt, 20),
          order: [['id', 'ASC']],
        });

        expect(result.map(row => row.get('id'))).to.deep.equal([2, 3]);
      });

      it('supports l1 distance filtering through helper methods', async function () {
        const result = await this.Item.findAll({
          attributes: ['id'],
          where: sql.where(sequelize.l1Distance('embeddings', [1, 2, 3]), Op.lt, 10),
          order: [['id', 'ASC']],
        });

        expect(result.map(row => row.get('id'))).to.deep.equal([1, 2, 4]);
      });

      it('supports native cosine similarity filtering', async function () {
        const result = await this.Item.findAll({
          attributes: ['id'],
          where: sql.where(
            sql.fn('VECTOR_COSINE_SIMILARITY', sql.attribute('embeddings'), [1, 2, 3]),
            Op.gt,
            0.99,
          ),
          order: [['id', 'ASC']],
        });

        expect(result.map(row => row.get('id'))).to.deep.equal([4]);
      });

      it('supports typed arrays in vector functions', async function () {
        const result = await this.Item.findAll({
          attributes: ['id'],
          where: sql.where(
            sequelize.l2Distance('embeddings', new Float32Array([1, 2, 3])),
            Op.lt,
            3,
          ),
          order: [['id', 'ASC']],
        });

        expect(result.map(row => row.get('id'))).to.deep.equal([1, 4]);
      });

      it('supports helper methods in ORDER BY', async function () {
        const result = await this.Item.findAll({
          attributes: ['id'],
          order: [sequelize.l2Distance('embeddings', [1, 2, 3])],
          limit: 1,
        });

        expect(result).to.have.length(1);
        expect(result[0].get('id')).to.equal(4);
      });
    });

    describe('INT vectors', () => {
      beforeEach(async function () {
        this.Item = sequelize.define(
          'SnowflakeIntVectorItem',
          {
            id: {
              type: DataTypes.INTEGER,
              primaryKey: true,
            },
            embeddings: DataTypes.VECTOR(3, 'int'),
          },
          {
            tableName: 'SnowflakeIntVectorItems',
            freezeTableName: true,
            timestamps: false,
          },
        );

        await this.Item.sync({ force: true });
        await sequelize.query(`
          INSERT INTO "SnowflakeIntVectorItems" ("id", "embeddings")
          VALUES
            (1, [1,2,3]::VECTOR(INT, 3)),
            (2, [4,5,6]::VECTOR(INT, 3)),
            (3, [9,9,9]::VECTOR(INT, 3))
        `);
      });

      it('supports integer typed arrays in vector functions', async function () {
        const result = await this.Item.findAll({
          attributes: ['id'],
          where: sql.where(sequelize.l2Distance('embeddings', new Int32Array([1, 2, 3])), Op.lt, 5),
          order: [['id', 'ASC']],
        });

        expect(result.map(row => row.get('id'))).to.deep.equal([1, 2]);
      });
    });
  });
}
