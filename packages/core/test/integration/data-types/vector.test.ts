import type {
  CreationOptional,
  InferAttributes,
  InferCreationAttributes,
  VectorValue,
} from '@sequelize/core';
import { DataTypes, Model } from '@sequelize/core';
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
        float32Embedding: DataTypes.VECTOR(3),
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

    expect(document.embedding).to.have.length(1536);
    expect(document.embedding[1024]).to.be.closeTo(embedding[1024], 1e-6);
  });

  it('allows sync({ alter: true }) when the VECTOR definition is unchanged', async () => {
    await expect(vars.VectorItem.sync({ alter: true })).to.be.fulfilled;
  });
});
