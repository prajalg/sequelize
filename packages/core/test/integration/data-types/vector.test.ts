import type {
  CreationOptional,
  InferAttributes,
  InferCreationAttributes,
  VectorValue,
} from '@sequelize/core';
import { DataTypes, Model, sql } from '@sequelize/core';
import { expect } from 'chai';
import semver from 'semver';
import { beforeEach2, getTestDialectTeaser, sequelize } from '../support';

const dialect = sequelize.dialect;

describe(getTestDialectTeaser('DataTypes.VECTOR'), () => {
  if (!dialect.supports.dataTypes.VECTOR) {
    return;
  }

  before(async function () {
    if (dialect.name !== 'oracle') {
      return;
    }

    const databaseVersion = semver.coerce(await sequelize.fetchDatabaseVersion());
    if (!databaseVersion || semver.lt(databaseVersion, '23.4.0')) {
      this.skip();
    }
  });

  const vars = beforeEach2(async () => {
    class VectorItem extends Model<
      InferAttributes<VectorItem>,
      InferCreationAttributes<VectorItem>
    > {
      declare id: CreationOptional<number>;
      declare float32Embedding: VectorValue;
      declare typedEmbedding: VectorValue;
      declare float64Embedding: VectorValue;
      declare int8Embedding: VectorValue;
      declare binaryEmbedding: VectorValue;
    }

    VectorItem.init(
      {
        id: {
          type: DataTypes.INTEGER,
          primaryKey: true,
          autoIncrement: true,
        },
        float32Embedding: {
          type: DataTypes.VECTOR(3),
          columnName: 'float32_embedding',
        },
        typedEmbedding: DataTypes.VECTOR({ dimensions: 3, typedArray: true }),
        float64Embedding: DataTypes.VECTOR({ dimensions: 3, elementType: 'float64' }),
        int8Embedding: DataTypes.VECTOR({ dimensions: 3, elementType: 'int8' }),
        binaryEmbedding: DataTypes.VECTOR({ dimensions: 24, elementType: 'binary' }),
      },
      { sequelize, timestamps: false },
    );

    await VectorItem.sync({ force: true });

    return { VectorItem };
  });

  it('round-trips plain arrays for each numeric element type', async () => {
    const item = await vars.VectorItem.create({
      float32Embedding: [1.25, 2.5, 3.75],
      typedEmbedding: [4, 5, 6],
      float64Embedding: [Number.EPSILON, Math.PI, Math.E],
      int8Embedding: [-128, 0, 127],
      binaryEmbedding: new Uint8Array([0b1010_1010, 0b0101_0101, 0b1111_0000]),
    });

    await item.reload();

    expect(item.float32Embedding).to.be.an('array');
    expect(item.float32Embedding).to.deep.equal([1.25, 2.5, 3.75]);
    expect(item.float64Embedding).to.be.an('array');
    expect(item.float64Embedding).to.deep.equal([Number.EPSILON, Math.PI, Math.E]);
    expect(item.int8Embedding).to.deep.equal([-128, 0, 127]);
    expect(item.typedEmbedding).to.be.instanceOf(Float32Array);
    expect(item.binaryEmbedding).to.be.instanceOf(Uint8Array);
    expect([...item.binaryEmbedding]).to.deep.equal([0b1010_1010, 0b0101_0101, 0b1111_0000]);
  });

  it('supports bulk inserts with plain vectors', async () => {
    await vars.VectorItem.bulkCreate([
      {
        float32Embedding: [1, 2, 3],
        typedEmbedding: [1, 2, 3],
        float64Embedding: [1, 2, 3],
        int8Embedding: [1, 2, 3],
        binaryEmbedding: new Uint8Array([1, 2, 3]),
      },
      {
        float32Embedding: [4, 5, 6],
        typedEmbedding: [4, 5, 6],
        float64Embedding: [4, 5, 6],
        int8Embedding: [4, 5, 6],
        binaryEmbedding: new Uint8Array([4, 5, 6]),
      },
    ]);

    expect(await vars.VectorItem.count()).to.equal(2);
  });

  it('updates and reloads a vector value', async () => {
    const item = await vars.VectorItem.create({
      float32Embedding: [1, 2, 3],
      typedEmbedding: [1, 2, 3],
      float64Embedding: [1, 2, 3],
      int8Embedding: [1, 2, 3],
      binaryEmbedding: new Uint8Array([1, 2, 3]),
    });

    item.float32Embedding = [4, 5, 6];
    await item.save();
    await item.reload();

    expect(item.float32Embedding).to.deep.equal([4, 5, 6]);
  });

  it('does not mark equal vectors as changed across array kinds', async () => {
    const item = await vars.VectorItem.create({
      float32Embedding: [1, 2, 3],
      typedEmbedding: [1, 2, 3],
      float64Embedding: [1, 2, 3],
      int8Embedding: [1, 2, 3],
      binaryEmbedding: new Uint8Array([1, 2, 3]),
    });

    await item.reload();
    item.float32Embedding = new Float32Array([1, 2, 3]);

    expect(item.changed('float32Embedding')).to.equal(false);
  });

  it('round-trips a realistic 1536-dimension embedding', async () => {
    class Document extends Model<InferAttributes<Document>, InferCreationAttributes<Document>> {
      declare id: CreationOptional<number>;
      declare embedding: VectorValue;
    }

    Document.init(
      {
        id: { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
        embedding: DataTypes.VECTOR(1536),
      },
      { sequelize, timestamps: false },
    );
    await Document.sync({ force: true });

    const embedding = Array.from({ length: 1536 }, (_, index) => index / 1536);
    const document = await Document.create({ embedding });
    await document.reload();

    const nearest = await Document.findOne({
      order: [sql.vectorDistance(sql.attribute('embedding'), embedding, 'cosine')],
    });

    expect(document.embedding).to.have.length(1536);
    expect(document.embedding[1024]).to.be.closeTo(embedding[1024], 1e-6);
    expect(nearest?.id).to.equal(document.id);
  });

  if (dialect.supports.vectorDistance) {
    it('orders rows by vector distance using a mapped attribute', async () => {
      const items = await vars.VectorItem.bulkCreate([
        {
          float32Embedding: [1, 0, 0],
          typedEmbedding: [1, 0, 0],
          float64Embedding: [1, 0, 0],
          int8Embedding: [1, 0, 0],
          binaryEmbedding: new Uint8Array([1, 0, 0]),
        },
        {
          float32Embedding: [0, 1, 0],
          typedEmbedding: [0, 1, 0],
          float64Embedding: [0, 1, 0],
          int8Embedding: [0, 1, 0],
          binaryEmbedding: new Uint8Array([0, 1, 0]),
        },
      ]);

      const nearest = await vars.VectorItem.findAll({
        order: [sql.vectorDistance(sql.attribute('float32Embedding'), [1, 0, 0], 'cosine')],
      });

      expect(nearest.map(item => item.id)).to.deep.equal([items[0].id, items[1].id]);
    });

    it('binds literal vectors using the column element type', async () => {
      const item = await vars.VectorItem.create({
        float32Embedding: [1, 0, 0],
        typedEmbedding: [1, 0, 0],
        float64Embedding: [1, 0, 0],
        int8Embedding: [1, 0, 0],
        binaryEmbedding: new Uint8Array([0b1111_0000, 0, 0]),
      });

      const float64Nearest = await vars.VectorItem.findOne({
        order: [sql.vectorDistance(sql.attribute('float64Embedding'), [1, 0, 0], 'euclidean')],
      });
      const int8Nearest = await vars.VectorItem.findOne({
        order: [sql.vectorDistance(sql.attribute('int8Embedding'), [1, 0, 0], 'manhattan')],
      });
      const binaryNearest = await vars.VectorItem.findOne({
        order: [
          sql.vectorDistance(
            sql.attribute('binaryEmbedding'),
            new Uint8Array([0b1111_0000, 0, 0]),
            'hamming',
          ),
        ],
      });

      expect(float64Nearest?.id).to.equal(item.id);
      expect(int8Nearest?.id).to.equal(item.id);
      expect(binaryNearest?.id).to.equal(item.id);
    });

    it('supports column-to-column vector distances', async () => {
      const item = await vars.VectorItem.create({
        float32Embedding: [1, 2, 3],
        typedEmbedding: [1, 2, 3],
        float64Embedding: [1, 2, 3],
        int8Embedding: [1, 2, 3],
        binaryEmbedding: new Uint8Array([1, 2, 3]),
      });

      const nearest = await vars.VectorItem.findOne({
        order: [
          sql.vectorDistance(
            sql.attribute('float32Embedding'),
            sql.attribute('typedEmbedding'),
            'cosine',
          ),
        ],
      });

      expect(nearest?.id).to.equal(item.id);
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

    it('treats dot as a distance with smaller values ranked first', async () => {
      const items = await vars.VectorItem.bulkCreate([
        {
          float32Embedding: [1, 0, 0],
          typedEmbedding: [1, 0, 0],
          float64Embedding: [1, 0, 0],
          int8Embedding: [1, 0, 0],
          binaryEmbedding: new Uint8Array([1, 0, 0]),
        },
        {
          float32Embedding: [-1, 0, 0],
          typedEmbedding: [-1, 0, 0],
          float64Embedding: [-1, 0, 0],
          int8Embedding: [-1, 0, 0],
          binaryEmbedding: new Uint8Array([0, 1, 0]),
        },
      ]);

      const nearest = await vars.VectorItem.findAll({
        order: [sql.vectorDistance(sql.attribute('float32Embedding'), [1, 0, 0], 'dot')],
      });

      expect(nearest.map(item => item.id)).to.deep.equal([items[0].id, items[1].id]);
    });
  }

  it('allows sync({ alter: true }) when the VECTOR definition is unchanged', async () => {
    await expect(vars.VectorItem.sync({ alter: true })).to.be.fulfilled;
  });

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

  if (dialect.supports.dataTypes.VECTOR.optionalDimensions) {
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
