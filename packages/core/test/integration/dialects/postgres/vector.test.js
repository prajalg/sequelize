'use strict';

const { DataTypes, Op, QueryTypes, sql } = require('@sequelize/core');
const { expect } = require('chai');
const semver = require('semver');
const { getTestDialect, sequelize } = require('../../../support');

if (getTestDialect() === 'postgres') {
  describe('[Postgres Specific] vectors', () => {
    before(async function () {
      try {
        await sequelize.query('CREATE EXTENSION IF NOT EXISTS vector');
        await sequelize.dialect.connectionManager.refreshDynamicOids();
      } catch (error) {
        console.log(error);
      }

      const extension = await sequelize.query(
        `SELECT extversion FROM pg_extension WHERE extname = 'vector'`,
        { type: QueryTypes.SELECT },
      );

      const rawVersion = extension[0]?.extversion;
      this.pgvectorVersion = semver.coerce(rawVersion)?.version ?? null;
    });

    describe('findAll', () => {
      beforeEach(async function () {
        this.Item = sequelize.define(
          'PgVectorItem',
          {
            embeddings: DataTypes.VECTOR(4),
          },
          {
            freezeTableName: true,
            timestamps: false,
          },
        );

        await this.Item.sync({ force: true });
        await this.Item.create({ embeddings: [1, 1, 1, 1] });
        await this.Item.create({ embeddings: new Float32Array([1, 2, 3, 3]) });
      });

      it('fetches rows', async function () {
        const result = await this.Item.findAll();

        expect(result).to.have.length(2);
      });

      it('returns arrays for vector columns', async function () {
        const result = await this.Item.findAll({ order: [['id', 'ASC']] });

        expect(result[0].getDataValue('embeddings')).to.deep.equal([1, 1, 1, 1]);
        expect(result[1].getDataValue('embeddings')).to.deep.equal([1, 2, 3, 3]);
      });
    });

    describe('similarity search functions', () => {
      beforeEach(async function () {
        this.Item = sequelize.define(
          'PgSimilarityItem',
          {
            embeddings: DataTypes.VECTOR(3),
          },
          {
            freezeTableName: true,
            timestamps: false,
          },
        );

        await this.Item.sync({ force: true });
        await this.Item.create({ embeddings: [1, 1, 1] });
        await this.Item.create({ embeddings: [5, 5, 5] });
        await this.Item.create({ embeddings: [10, 10, 10] });
        await this.Item.create({ embeddings: [1, 2, 3] });
      });

      it('supports cosine distance filtering', async function () {
        const queryVector = [1, 2, 3];
        const result = await this.Item.findAll({
          where: sql.where(sequelize.cosineDistance('embeddings', queryVector), Op.lt, 0.01),
        });

        expect(result).to.have.length(1);
      });

      it('supports inner product filtering', async function () {
        const queryVector = [1, 2, 3];
        const result = await this.Item.findAll({
          where: sql.where(sequelize.innerProduct('embeddings', queryVector), Op.gt, 20),
        });

        expect(result).to.have.length(2);
      });

      it('supports l2 distance filtering', async function () {
        const queryVector = [1, 2, 3];
        const result = await this.Item.findAll({
          where: sql.where(sequelize.l2Distance('embeddings', queryVector), Op.lt, 3),
        });

        expect(result).to.have.length(2);
      });

      it('supports vectorDistance filtering', async function () {
        const queryVector = [1, 2, 3];
        const result = await this.Item.findAll({
          where: sql.where(sequelize.vectorDistance('embeddings', queryVector), Op.lt, 3),
        });

        expect(result).to.have.length(2);
      });

      it('supports l1 distance filtering when pgvector provides it', async function () {
        if (!this.pgvectorVersion || !semver.gte(this.pgvectorVersion, '0.7.0')) {
          this.skip();
        }

        const queryVector = [1, 2, 3];
        const result = await this.Item.findAll({
          where: sql.where(sequelize.l1Distance('embeddings', queryVector), Op.lt, 10),
        });

        expect(result).to.have.length(3);
      });

      it('accepts typed arrays in vector functions', async function () {
        const result = await this.Item.findAll({
          where: sql.where(
            sequelize.l2Distance('embeddings', new Float32Array([1, 2, 3])),
            Op.lt,
            3,
          ),
        });

        expect(result).to.have.length(2);
      });

      it('supports helper methods in ORDER BY', async function () {
        const queryVector = [1, 2, 3];
        const result = await this.Item.findAll({
          order: [sequelize.l2Distance('embeddings', queryVector)],
          limit: 1,
        });

        expect(result).to.have.length(1);
        expect(result[0].getDataValue('embeddings')).to.deep.equal([1, 2, 3]);
      });
    });
  });
}
