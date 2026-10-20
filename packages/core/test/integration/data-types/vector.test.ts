import type {
  CreationOptional,
  InferAttributes,
  InferCreationAttributes,
  VectorElementType,
  VectorMetric,
  VectorValue,
} from '@sequelize/core';
import { DataTypes, Model, sql } from '@sequelize/core';
import { expect } from 'chai';
import semver from 'semver';
import { beforeEach2, getTestDialectTeaser, sequelize } from '../support';

const dialect = sequelize.dialect;

describe(getTestDialectTeaser('DataTypes.VECTOR'), () => {
  const vectorSupport = dialect.supports.dataTypes.VECTOR;
  if (!vectorSupport) {
    return;
  }

  if (dialect.name === 'oracle') {
    before(async function checkOracleVersionForVectorSupport() {
      const databaseVersion = semver.coerce(await sequelize.fetchDatabaseVersion());

      if (!databaseVersion || semver.lt(databaseVersion, '23.4.0')) {
        this.skip();
      }
    });
  }

  const allVectorTestCases: ReadonlyArray<{
    elementType: VectorElementType;
    dimensions: number;
    initialValue: VectorValue;
    updatedValue: VectorValue;
    typedValue: VectorValue;
  }> = [
    {
      elementType: 'float16',
      dimensions: 3,
      initialValue: [1.25, 2.5, 3.75],
      updatedValue: [4, 5, 6],
      typedValue: new Float32Array([1.25, 2.5, 3.75]),
    },
    {
      elementType: 'float32',
      dimensions: 3,
      initialValue: [1.25, 2.5, 3.75],
      updatedValue: [4, 5, 6],
      typedValue: new Float32Array([1.25, 2.5, 3.75]),
    },
    {
      elementType: 'float64',
      dimensions: 3,
      initialValue: [Number.EPSILON, Math.PI, Math.E],
      updatedValue: [4, 5, 6],
      typedValue: new Float64Array([Number.EPSILON, Math.PI, Math.E]),
    },
    {
      elementType: 'int8',
      dimensions: 3,
      initialValue: [-128, 0, 127],
      updatedValue: [4, 5, 6],
      typedValue: new Int8Array([-128, 0, 127]),
    },
    {
      elementType: 'binary',
      dimensions: 24,
      initialValue: new Uint8Array([0b1010_1010, 0b0101_0101, 0b1111_0000]),
      updatedValue: new Uint8Array([1, 2, 3]),
      typedValue: new Uint8Array([0b1010_1010, 0b0101_0101, 0b1111_0000]),
    },
  ];
  const vectorTestCases = allVectorTestCases.filter(
    testCase => vectorSupport.elementTypes[testCase.elementType],
  );

  for (const testCase of vectorTestCases) {
    describe(`${testCase.elementType} vectors`, () => {
      const vars = beforeEach2(async () => {
        class VectorItem extends Model<
          InferAttributes<VectorItem>,
          InferCreationAttributes<VectorItem>
        > {
          declare id: CreationOptional<number>;
          declare embedding: VectorValue;
          declare typedEmbedding: VectorValue;
        }

        VectorItem.init(
          {
            id: {
              type: DataTypes.INTEGER,
              primaryKey: true,
              autoIncrement: true,
            },
            embedding: {
              type: DataTypes.VECTOR({
                dimensions: testCase.dimensions,
                elementType: testCase.elementType,
              }),
              columnName: 'embedding_value',
            },
            typedEmbedding: DataTypes.VECTOR({
              dimensions: testCase.dimensions,
              elementType: testCase.elementType,
              typedArray: true,
            }),
          },
          { sequelize, timestamps: false },
        );

        await VectorItem.sync({ force: true });

        return { VectorItem };
      });

      it('round-trips plain and typed values', async () => {
        const item = await vars.VectorItem.create({
          embedding: testCase.initialValue,
          typedEmbedding: testCase.initialValue,
        });

        await item.reload();

        if (testCase.elementType === 'binary') {
          expect(item.embedding).to.deep.equal(testCase.initialValue);
        } else {
          expect(item.embedding).to.deep.equal([...testCase.initialValue]);
        }

        expect(item.typedEmbedding).to.deep.equal(testCase.typedValue);
      });

      it('supports bulk inserts', async () => {
        await vars.VectorItem.bulkCreate([
          { embedding: testCase.initialValue, typedEmbedding: testCase.initialValue },
          { embedding: testCase.updatedValue, typedEmbedding: testCase.updatedValue },
        ]);

        expect(await vars.VectorItem.count()).to.equal(2);
      });

      it('updates and reloads a vector value', async () => {
        const item = await vars.VectorItem.create({
          embedding: testCase.initialValue,
          typedEmbedding: testCase.initialValue,
        });

        item.embedding = testCase.updatedValue;
        await item.save();
        await item.reload();

        if (testCase.elementType === 'binary') {
          expect(item.embedding).to.deep.equal(testCase.updatedValue);
        } else {
          expect(item.embedding).to.deep.equal([...testCase.updatedValue]);
        }
      });

      it('does not mark equal vectors as changed across array kinds', async () => {
        const item = await vars.VectorItem.create({
          embedding: testCase.initialValue,
          typedEmbedding: testCase.initialValue,
        });

        await item.reload();
        item.embedding = testCase.typedValue;

        expect(item.changed('embedding')).to.equal(false);
      });
    });
  }

  const vectorDistanceSupport = dialect.supports.vectorDistance;
  const supportsDistanceMetric = (metric: string, elementType: VectorElementType) =>
    vectorDistanceSupport &&
    vectorDistanceSupport.metrics.includes(metric as never) &&
    vectorSupport.elementTypes[elementType];

  if (supportsDistanceMetric('cosine', 'float32')) {
    describe('cosine vector distance', () => {
      const vars = beforeEach2(async () => {
        class VectorItem extends Model<
          InferAttributes<VectorItem>,
          InferCreationAttributes<VectorItem>
        > {
          declare id: CreationOptional<number>;
          declare embedding: VectorValue;
          declare comparisonEmbedding: VectorValue;
        }

        VectorItem.init(
          {
            id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
            embedding: {
              type: DataTypes.VECTOR(3),
              columnName: 'embedding_vector',
            },
            comparisonEmbedding: DataTypes.VECTOR(3),
          },
          { sequelize, timestamps: false },
        );
        await VectorItem.sync({ force: true });

        return { VectorItem };
      });

      it('orders rows by distance using a mapped attribute', async () => {
        const items = await vars.VectorItem.bulkCreate([
          { embedding: [1, 0, 0], comparisonEmbedding: [1, 0, 0] },
          { embedding: [0, 1, 0], comparisonEmbedding: [0, 1, 0] },
        ]);

        const nearest = await vars.VectorItem.findAll({
          order: [sql.vectorDistance(sql.attribute('embedding'), [1, 0, 0], 'cosine')],
        });

        expect(nearest.map(item => item.id)).to.deep.equal([items[0].id, items[1].id]);
      });

      it('supports column-to-column vector distances', async () => {
        const item = await vars.VectorItem.create({
          embedding: [1, 2, 3],
          comparisonEmbedding: [1, 2, 3],
        });

        const nearest = await vars.VectorItem.findOne({
          order: [
            sql.vectorDistance(
              sql.attribute('embedding'),
              sql.attribute('comparisonEmbedding'),
              'cosine',
            ),
          ],
        });

        expect(nearest?.id).to.equal(item.id);
      });
    });

    it('resolves mapped VECTOR attributes through an include', async () => {
      class VectorCollection extends Model {}

      class IncludedVectorItem extends Model {}

      VectorCollection.init(
        { id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true } },
        { sequelize, timestamps: false },
      );
      IncludedVectorItem.init(
        {
          id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
          collectionId: DataTypes.INTEGER,
          embedding: {
            type: DataTypes.VECTOR(3),
            columnName: 'embedding_vector',
          },
        },
        { sequelize, timestamps: false },
      );
      VectorCollection.hasMany(IncludedVectorItem, {
        as: 'items',
        foreignKey: 'collectionId',
      });
      await sequelize.sync({ force: true });

      const collection = await VectorCollection.create();
      await IncludedVectorItem.create({
        collectionId: collection.get('id'),
        embedding: [1, 0, 0],
      });

      const result = await VectorCollection.findAll({
        include: [{ association: 'items' }],
        order: [sql.vectorDistance(sql.attribute('$items.embedding$'), [1, 0, 0], 'cosine')],
      });

      expect(result).to.have.length(1);
    });
  }

  const distanceTestCases: ReadonlyArray<{
    elementType: VectorElementType;
    dimensions: number;
    metric: VectorMetric;
    value: VectorValue;
  }> = [
    { elementType: 'float64' as const, dimensions: 3, metric: 'euclidean', value: [1, 0, 0] },
    { elementType: 'int8' as const, dimensions: 3, metric: 'manhattan', value: [1, 0, 0] },
    {
      elementType: 'binary' as const,
      dimensions: 24,
      metric: 'hamming',
      value: new Uint8Array([0b1111_0000, 0, 0]),
    },
  ];

  for (const testCase of distanceTestCases) {
    if (!supportsDistanceMetric(testCase.metric, testCase.elementType)) {
      continue;
    }

    it(`binds ${testCase.elementType} literal vectors for ${testCase.metric} distance`, async () => {
      class VectorItem extends Model<
        InferAttributes<VectorItem>,
        InferCreationAttributes<VectorItem>
      > {
        declare id: CreationOptional<number>;
        declare embedding: VectorValue;
      }

      VectorItem.init(
        {
          id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
          embedding: DataTypes.VECTOR({
            dimensions: testCase.dimensions,
            elementType: testCase.elementType,
          }),
        },
        { sequelize, timestamps: false },
      );
      await VectorItem.sync({ force: true });
      const item = await VectorItem.create({ embedding: testCase.value });

      const nearest = await VectorItem.findOne({
        order: [sql.vectorDistance(sql.attribute('embedding'), testCase.value, testCase.metric)],
      });

      expect(nearest?.id).to.equal(item.id);
    });
  }

  if (supportsDistanceMetric('dot', 'float32')) {
    it('treats dot as a distance with smaller values ranked first', async () => {
      class VectorItem extends Model<
        InferAttributes<VectorItem>,
        InferCreationAttributes<VectorItem>
      > {
        declare id: CreationOptional<number>;
        declare embedding: VectorValue;
      }

      VectorItem.init(
        {
          id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
          embedding: DataTypes.VECTOR(3),
        },
        { sequelize, timestamps: false },
      );
      await VectorItem.sync({ force: true });
      const items = await VectorItem.bulkCreate([
        { embedding: [1, 0, 0] },
        { embedding: [-1, 0, 0] },
      ]);

      const nearest = await VectorItem.findAll({
        order: [sql.vectorDistance(sql.attribute('embedding'), [1, 0, 0], 'dot')],
      });

      expect(nearest.map(item => item.id)).to.deep.equal([items[0].id, items[1].id]);
    });
  }

  const largeVectorTestCase = vectorTestCases.find(
    testCase =>
      testCase.elementType !== 'binary' &&
      vectorSupport.elementTypes[testCase.elementType]!.maxDimensions >= 1536,
  );
  if (largeVectorTestCase) {
    it('round-trips a realistic 1536-dimension embedding', async () => {
      class Document extends Model<InferAttributes<Document>, InferCreationAttributes<Document>> {
        declare id: CreationOptional<number>;
        declare embedding: VectorValue;
      }

      Document.init(
        {
          id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
          embedding: DataTypes.VECTOR({
            dimensions: 1536,
            elementType: largeVectorTestCase.elementType,
          }),
        },
        { sequelize, timestamps: false },
      );
      await Document.sync({ force: true });

      const embedding = Array.from({ length: 1536 }, (_, index) => index / 1536);
      const document = await Document.create({ embedding });
      await document.reload();

      expect(document.embedding).to.have.length(1536);
      expect(document.embedding[1024]).to.be.closeTo(embedding[1024], 1e-6);
    });
  }

  const defaultVectorTestCase = vectorTestCases[0];
  if (defaultVectorTestCase) {
    it('allows sync({ alter: true }) when the VECTOR definition is unchanged', async () => {
      class VectorItem extends Model {}

      VectorItem.init(
        {
          id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
          embedding: DataTypes.VECTOR({
            dimensions: defaultVectorTestCase.dimensions,
            elementType: defaultVectorTestCase.elementType,
          }),
        },
        { sequelize, timestamps: false },
      );
      await VectorItem.sync({ force: true });

      await expect(VectorItem.sync({ alter: true })).to.be.fulfilled;
    });
  }

  if (dialect.name === 'oracle') {
    it('rejects changing an existing VECTOR definition through sync({ alter: true })', async () => {
      class OriginalVectorItem extends Model {}

      OriginalVectorItem.init(
        {
          id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
          embedding: DataTypes.VECTOR(3),
        },
        { sequelize, tableName: 'vector_alter_items', timestamps: false },
      );
      await OriginalVectorItem.sync({ force: true });

      class ChangedVectorItem extends Model {}

      ChangedVectorItem.init(
        {
          id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
          embedding: DataTypes.VECTOR(4),
        },
        { sequelize, tableName: 'vector_alter_items', timestamps: false },
      );

      await expect(ChangedVectorItem.sync({ alter: true })).to.be.rejectedWith(
        'Changing Oracle VECTOR column embedding from VECTOR(3, FLOAT32) to VECTOR(4, FLOAT32) is not supported by sync({ alter: true })',
      );
    });
  }

  if (vectorSupport.optionalDimensions) {
    it('round-trips a vector without fixed dimensions', async () => {
      class FlexibleVectorItem extends Model<
        InferAttributes<FlexibleVectorItem>,
        InferCreationAttributes<FlexibleVectorItem>
      > {
        declare id: CreationOptional<number>;
        declare embedding: VectorValue;
      }

      FlexibleVectorItem.init(
        {
          id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
          embedding: DataTypes.VECTOR(),
        },
        { sequelize, timestamps: false },
      );
      await FlexibleVectorItem.sync({ force: true });

      const item = await FlexibleVectorItem.create({ embedding: [1, 2, 3, 4] });
      await item.reload();

      expect(item.embedding).to.deep.equal([1, 2, 3, 4]);
    });
  }
});
