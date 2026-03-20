'use strict';

const { DataTypes, Op, sql } = require('@sequelize/core');
const { expect } = require('chai');
const semver = require('semver');
const { getTestDialect, sequelize } = require('../../../support');

if (getTestDialect() === 'oracle') {
  describe('[Oracle Specific] vectors', () => {
    before(async function () {
      const rawVersion = await sequelize.fetchDatabaseVersion();
      const normalized = semver.coerce(rawVersion)?.version; // e.g. 23.26.0

      if (!normalized || !semver.gte(normalized, '23.4.0')) {
        this.skip();
      }
    });

    describe('findAll', () => {
      beforeEach(async function () {
        this.Item = sequelize.define('Item', {
          embeddings: DataTypes.VECTOR(4),
        });

        await this.Item.sync({ force: true });
        await this.Item.create({ embeddings: new Float32Array([1, 1, 1, 1]) });
        await this.Item.create({ embeddings: new Float32Array([1, 2, 3, 3]) });
      });

      it('fetches rows', async function () {
        const result = await this.Item.findAll();
        expect(result).to.have.length(2);
      });

      it('returns typed arrays for vector column', async function () {
        const result = await this.Item.findAll();
        expect(result[0].getDataValue('embeddings').BYTES_PER_ELEMENT).to.equal(4);
      });
    });

    describe('similarity search functions', () => {
      beforeEach(async function () {
        this.Item = sequelize.define('Item', {
          embeddings: DataTypes.VECTOR(3),
        });

        await this.Item.sync({ force: true });
        await this.Item.create({ embeddings: new Float32Array([1, 1, 1]) });
        await this.Item.create({ embeddings: new Float32Array([5, 5, 5]) });
        await this.Item.create({ embeddings: new Float32Array([10, 10, 10]) });
        await this.Item.create({ embeddings: new Float32Array([1, 2, 3]) });
      });

      it('supports l2 distance filtering', async function () {
        const queryVector = [1, 2, 3];
        const result = await this.Item.findAll({
          where: sql.where(
            sql.fn('L2_DISTANCE', sql.attribute('embeddings'), queryVector),
            Op.lt,
            3,
          ),
        });

        expect(result.length).to.equal(2);
      });

      it('supports helper methods', async function () {
        const queryVector = [1, 2, 3];
        const result = await this.Item.findAll({
          where: sql.where(sequelize.vectorDistance('embeddings', queryVector), Op.lt, 2),
        });

        expect(result.length).to.equal(4);
      });
    });
  });
}
