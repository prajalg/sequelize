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

      it('supports cosine distance filtering', async function () {
        const queryVector = [1, 2, 3];
        const result = await this.Item.findAll({
          where: sql.where(
            sql.fn('COSINE_DISTANCE', sql.attribute('embeddings'), queryVector),
            Op.lt,
            0.01,
          ),
        });

        expect(result.length).to.equal(1);
      });

      it('supports inner product filtering', async function () {
        const queryVector = [1, 2, 3];
        const result = await this.Item.findAll({
          where: sql.where(
            sql.fn('INNER_PRODUCT', sql.attribute('embeddings'), queryVector),
            Op.gt,
            20,
          ),
        });

        expect(result.length).to.equal(2);
      });

      it('supports l1 distance filtering', async function () {
        const queryVector = [1, 2, 3];
        const result = await this.Item.findAll({
          where: sql.where(
            sql.fn('L1_DISTANCE', sql.attribute('embeddings'), queryVector),
            Op.lt,
            10,
          ),
        });

        expect(result.length).to.equal(3);
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

      it('supports all helper methods in where filters', async function () {
        const queryVector = [1, 2, 3];
        const helperCases = [
          { name: 'cosineDistance', operator: Op.lt, threshold: 0.01, expected: 1 },
          { name: 'innerProduct', operator: Op.gt, threshold: 10, expected: 3 },
          { name: 'l1Distance', operator: Op.lt, threshold: 10, expected: 3 },
          { name: 'l2Distance', operator: Op.lt, threshold: 6, expected: 3 },
          { name: 'vectorDistance', operator: Op.lt, threshold: 0.01, expected: 1 },
        ];

        for (const helperCase of helperCases) {
          const result = await this.Item.findAll({
            where: sql.where(
              sequelize[helperCase.name]('embeddings', queryVector),
              helperCase.operator,
              helperCase.threshold,
            ),
          });

          expect(result.length, helperCase.name).to.equal(helperCase.expected);
        }
      });

      it('accepts valid VECTOR literal strings in vector functions', async function () {
        const result = await this.Item.findAll({
          where: sql.where(
            sql.fn('VECTOR_DISTANCE', sql.attribute('embeddings'), `VECTOR('[1,2,3]')`),
            Op.lt,
            2,
          ),
        });

        expect(result.length).to.equal(4);
      });

      it('rejects malformed VECTOR literal strings in vector functions', async function () {
        await expect(
          this.Item.findAll({
            where: sql.where(
              sql.fn('VECTOR_DISTANCE', sql.attribute('embeddings'), `VECTR('[1,2,3]')`),
              Op.lt,
              2,
            ),
          }),
        ).to.be.rejected;
      });

      it('supports all vector helper methods in ORDER BY', async function () {
        const queryVector = [1, 2, 3];
        const helpers = [
          'cosineDistance',
          'innerProduct',
          'l1Distance',
          'l2Distance',
          'vectorDistance',
        ];

        for (const helper of helpers) {
          const result = await this.Item.findAll({
            order: [sequelize[helper]('embeddings', queryVector)],
            limit: 1,
          });

          expect(result).to.have.length(1);
        }
      });
    });

    describe('vector input validation and persistence', () => {
      beforeEach(async function () {
        this.Item = sequelize.define('VectorInputItem', {
          embeddings: DataTypes.VECTOR(3),
        });

        await this.Item.sync({ force: true });
      });

      const acceptedInputs = [
        { name: 'number array', value: [1, 2, 3] },
        { name: 'Float32Array', value: new Float32Array([1, 2, 3]) },
        { name: 'Float64Array', value: new Float64Array([1, 2, 3]) },
        { name: 'Int8Array', value: new Int8Array([1, 2, 3]) },
      ];

      for (const { name, value } of acceptedInputs) {
        it(`accepts ${name} input`, async function () {
          await this.Item.create({ embeddings: value });

          const row = await this.Item.findOne();
          expect(Array.from(row.getDataValue('embeddings'))).to.deep.equal([1, 2, 3]);
        });
      }

      it('rejects DataView input', async function () {
        const dataView = new DataView(new ArrayBuffer(3));

        await expect(this.Item.create({ embeddings: dataView })).to.be.rejectedWith(
          Error,
          'is not a valid vector',
        );
      });

      it('rejects string input', async function () {
        await expect(this.Item.create({ embeddings: '1,2,3' })).to.be.rejectedWith(
          Error,
          'is not a valid vector',
        );
      });

      it('rejects arrays with non-number values', async function () {
        await expect(this.Item.create({ embeddings: [1, '2', 3] })).to.be.rejectedWith(
          Error,
          'is not a valid vector',
        );
      });

      it('stores Uint8Array in binary vectors with matching bit-dimension', async () => {
        const BinaryItem = sequelize.define('BinaryVectorInputItem', {
          embeddings: DataTypes.VECTOR(24, 'binary'),
        });

        await BinaryItem.sync({ force: true });
        await BinaryItem.create({ embeddings: new Uint8Array([1, 2, 3]) });

        const row = await BinaryItem.findOne();
        expect(row).to.not.equal(null);
      });
    });

    describe('vector indexes', () => {
      it('creates vector index from model indexes option during sync', async () => {
        const indexName = 'vector_input_item_embeddings_hnsw_idx';
        const IndexedItem = sequelize.define(
          'VectorIndexedItem',
          {
            embeddings: DataTypes.VECTOR(3),
          },
          {
            indexes: [
              {
                name: indexName,
                type: 'VECTOR',
                fields: ['embeddings'],
                using: 'hnsw',
                parameter: { neighbor: 8, efconstruction: 32 },
              },
            ],
          },
        );

        try {
          await IndexedItem.sync({ force: true });

          const indexes = await sequelize.queryInterface.showIndex(IndexedItem.table);
          const vectorIndex = indexes.find(
            index => index.name?.toLowerCase() === indexName.toLowerCase(),
          );

          expect(vectorIndex).to.not.equal(undefined);
          expect(vectorIndex.type).to.equal('VECTOR');
        } finally {
          try {
            await sequelize.queryInterface.removeIndex(IndexedItem.table, indexName);
          } finally {
            await IndexedItem.drop();
          }
        }
      });
    });
  });
}
