import type { AttributeOptions, DataType } from '@sequelize/core';
import { DataTypes, Model, QueryTypes, sql } from '@sequelize/core';
import { expect } from 'chai';
import semver from 'semver';
import { getTestDialectTeaser, sequelize } from '../../support';

describe(getTestDialectTeaser('Oracle VECTOR column alterations'), () => {
  if (sequelize.dialect.name !== 'oracle' || !sequelize.dialect.supports.dataTypes.VECTOR) {
    return;
  }

  before(async function checkOracleVectorVersion() {
    const version = semver.coerce(await sequelize.fetchDatabaseVersion());
    if (!version || semver.lt(version, '23.4.0')) {
      this.skip();
    }
  });

  const tableName = 'vector_column_options';

  // Oracle VECTOR schema restrictions (defaults, primary/foreign keys and MODIFY):
  // https://docs.oracle.com/en/database/oracle/oracle-database/26/vecse/create-tables-using-vector-data-type.html

  async function createOriginal(embedding: AttributeOptions | DataType = DataTypes.VECTOR(3)) {
    class OriginalItem extends Model {}

    OriginalItem.init(
      {
        id: { type: DataTypes.INTEGER, primaryKey: true },
        embedding,
      },
      { sequelize, tableName, timestamps: false },
    );
    await OriginalItem.sync({ force: true });

    return OriginalItem;
  }

  function changedModel(embedding: AttributeOptions | DataType) {
    class ChangedItem extends Model {}

    ChangedItem.init(
      {
        id: { type: DataTypes.INTEGER, primaryKey: true },
        embedding,
      },
      { sequelize, tableName, timestamps: false },
    );

    return ChangedItem;
  }

  for (const [name, type] of [
    ['element type', DataTypes.VECTOR({ dimensions: 3, elementType: 'float64' })],
    ['flexible dimensions', DataTypes.VECTOR()],
  ] as const) {
    it(`rejects changing VECTOR ${name} and preserves the existing definition`, async () => {
      await createOriginal();

      await expect(changedModel(type).sync({ alter: true })).to.be.rejectedWith(
        'Use an explicit migration',
      );
      const description = await sequelize.queryInterface.describeTable(tableName);
      expect(description.embedding.type).to.equal('VECTOR(3, FLOAT32)');
    });
  }

  for (const [name, options, message] of [
    ['array default', { defaultValue: [1, 2, 3] }, 'does not support database defaults'],
    [
      'SQL default',
      { defaultValue: sql.literal("TO_VECTOR('[1,2,3]', 3, FLOAT32)") },
      'does not support database defaults',
    ],
    ['NULL default', { defaultValue: null }, 'does not support database defaults'],
    ['primary key', { primaryKey: true }, 'cannot be used as a primary key'],
    [
      'foreign key',
      { references: { table: tableName, key: 'id' } },
      'cannot be used as a foreign key',
    ],
    ['auto increment', { autoIncrement: true }, 'does not support autoIncrement'],
  ] as const) {
    it(`rejects a VECTOR ${name} before sync executes SQL`, async () => {
      await createOriginal();
      const statements: string[] = [];
      await expect(
        changedModel({ type: DataTypes.VECTOR(3), ...options }).sync({
          alter: true,
          logging: statement => statements.push(statement),
        }),
      ).to.be.rejectedWith(`Oracle VECTOR column embedding ${message}.`);
      expect(statements).to.deep.equal([]);
    });
  }

  it('reports an error when adding uniqueness to a VECTOR column', async () => {
    await createOriginal();

    await expect(changedModel({ type: DataTypes.VECTOR(3), unique: true }).sync({ alter: true })).to
      .be.rejected;
    const indexes = await sequelize.queryInterface.showIndex(tableName);
    expect(
      indexes.some(index => index.fields.some(field => field.attribute === 'embedding')),
    ).to.equal(false);
  });

  it('rejects an invalid VECTOR before force sync can drop the existing table', async () => {
    const Original = await createOriginal();
    await Original.create({ id: 1, embedding: [1, 2, 3] });
    const statements: string[] = [];

    await expect(
      changedModel({ type: DataTypes.VECTOR(3), primaryKey: true }).sync({
        force: true,
        hooks: false,
        logging: statement => statements.push(statement),
      }),
    ).to.be.rejectedWith('Oracle VECTOR column embedding cannot be used as a primary key.');
    expect(statements).to.deep.equal([]);
    expect((await Original.findByPk(1))?.get('embedding')).to.deep.equal([1, 2, 3]);
  });

  it('allows JavaScript default factories and keeps database defaults unset', async () => {
    const Item = changedModel({ type: DataTypes.VECTOR(3), defaultValue: () => [1, 2, 3] });
    await Item.sync({ force: true });
    await Item.sync({ alter: true });
    const item = await Item.create({ id: 1 });
    await item.reload();

    expect(item.get('embedding')).to.deep.equal([1, 2, 3]);
    const rows = await sequelize.query<{ DATA_DEFAULT: string | null }>(
      'SELECT DATA_DEFAULT FROM USER_TAB_COLUMNS WHERE TABLE_NAME = :tableName AND COLUMN_NAME = :columnName',
      { replacements: { tableName, columnName: 'embedding' }, type: QueryTypes.SELECT },
    );
    expect(rows[0].DATA_DEFAULT).to.equal(null);
  });

  it('allows changing the read representation without changing the VECTOR column', async () => {
    const Original = await createOriginal();
    await Original.create({ id: 1, embedding: [1, 2, 3] });
    const Changed = changedModel(DataTypes.VECTOR({ dimensions: 3, typedArray: true }));

    await Changed.sync({ alter: true });
    expect((await Changed.findByPk(1))?.get('embedding')).to.deep.equal(
      new Float32Array([1, 2, 3]),
    );
  });

  it('rejects converting a scalar column to VECTOR and preserves its data', async () => {
    const Original = await createOriginal(DataTypes.INTEGER);
    await Original.create({ id: 1, embedding: 7 });

    await expect(changedModel(DataTypes.VECTOR(3)).sync({ alter: true })).to.be.rejectedWith(
      'Use an explicit migration',
    );
    expect((await Original.findByPk(1))?.get('embedding')).to.equal(7);
  });

  it('rejects converting VECTOR to a scalar column and preserves its data', async () => {
    const Original = await createOriginal();
    await Original.create({ id: 1, embedding: [1, 2, 3] });

    await expect(changedModel(DataTypes.INTEGER).sync({ alter: true })).to.be.rejected;
    expect((await Original.findByPk(1))?.get('embedding')).to.deep.equal([1, 2, 3]);
  });

  it('keeps data when a JavaScript attribute is renamed but its physical column stays the same', async () => {
    const Original = await createOriginal();
    await Original.create({ id: 1, embedding: [1, 2, 3] });

    class RenamedItem extends Model {}

    RenamedItem.init(
      {
        id: { type: DataTypes.INTEGER, primaryKey: true },
        renamedEmbedding: { type: DataTypes.VECTOR(3), columnName: 'embedding' },
      },
      { sequelize, tableName, timestamps: false },
    );

    await RenamedItem.sync({ alter: true });
    expect((await RenamedItem.findByPk(1))?.get('renamedEmbedding')).to.deep.equal([1, 2, 3]);
  });
});
