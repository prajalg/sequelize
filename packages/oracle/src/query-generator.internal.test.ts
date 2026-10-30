import { DataTypes, Sequelize, sql } from '@sequelize/core';
import { OracleDialect } from '@sequelize/oracle';
import { expect } from 'chai';

describe('Oracle query generator internals', () => {
  const sequelize = new Sequelize({ dialect: OracleDialect });
  const queryGenerator = sequelize.queryGenerator;

  describe('Oracle VECTOR column options', () => {
    const vector = sequelize.normalizeDataType(DataTypes.VECTOR(3));
    const validate = (options: object) =>
      queryGenerator._validateAttributes({ embedding: { type: vector, ...options } });

    // Oracle VECTOR restrictions, including defaults and primary/foreign keys:
    // https://docs.oracle.com/en/database/oracle/oracle-database/26/vecse/create-tables-using-vector-data-type.html
    for (const [name, options, message] of [
      ['array default', { defaultValue: [1, 2, 3] }, 'does not support database defaults'],
      ['NULL default', { defaultValue: null }, 'does not support database defaults'],
      ['SQL default', { defaultValue: sql.literal('NULL') }, 'does not support database defaults'],
      ['primary key', { primaryKey: true }, 'cannot be used as a primary key'],
      [
        'foreign key',
        { references: { table: 'items', key: 'id' } },
        'cannot be used as a foreign key',
      ],
      ['auto increment', { autoIncrement: true }, 'does not support autoIncrement'],
    ] as const) {
      it(`rejects a VECTOR ${name}`, () => {
        expect(() => validate(options)).to.throw(`Oracle VECTOR column embedding ${message}.`);
      });
    }

    it('allows JavaScript default functions without evaluating them', () => {
      expect(() =>
        validate({
          defaultValue: () => {
            throw new Error('Schema validation must not evaluate application defaults');
          },
        }),
      ).not.to.throw();
    });

    it('allows nullable and non-nullable ordinary VECTOR columns', () => {
      expect(() =>
        validate({ allowNull: true, primaryKey: false, autoIncrement: false }),
      ).not.to.throw();
      expect(() => validate({ allowNull: false })).not.to.throw();
    });

    it('leaves non-VECTOR attribute options to their existing validation', () => {
      expect(() =>
        queryGenerator._validateAttributes({
          id: {
            type: sequelize.normalizeDataType(DataTypes.INTEGER),
            defaultValue: 1,
            primaryKey: true,
          },
        }),
      ).not.to.throw();
    });
  });

  it('validates VECTOR options for model preflight and direct column SQL', () => {
    const attribute = {
      type: sequelize.normalizeDataType(DataTypes.VECTOR(3)),
      columnName: 'embedding_vector',
      defaultValue: [1, 2, 3],
    };

    expect(() => queryGenerator._validateAttributes({ embedding: attribute })).to.throw(
      'Oracle VECTOR column embedding_vector does not support database defaults.',
    );
    expect(() => queryGenerator.attributeToSql(attribute)).to.throw(
      'Oracle VECTOR column embedding_vector does not support database defaults.',
    );
  });

  const invalidDefault = { type: DataTypes.VECTOR(3), defaultValue: [1, 2, 3] };
  for (const [name, operation] of Object.entries({
    createTable: async () =>
      sequelize.queryInterface.createTable('items', { embedding: invalidDefault }),
    addColumn: async () => sequelize.queryInterface.addColumn('items', 'embedding', invalidDefault),
    changeColumn: async () =>
      sequelize.queryInterface.changeColumn('items', 'embedding', invalidDefault),
  })) {
    it(`rejects unsupported VECTOR options before ${name} reaches the database`, async () => {
      const error = await operation().catch((caughtError: unknown) => caughtError);

      // No connection is configured: validation must fail before accessing the database.
      expect(error).to.be.instanceOf(Error);
      expect((error as Error).message).to.equal(
        'Oracle VECTOR column embedding does not support database defaults.',
      );
    });
  }

  it('uses Oracle VECTOR bindings in insert and update queries', () => {
    const attributes = {
      embedding: {
        attributeName: 'embedding',
        columnName: 'embedding',
        fieldName: 'embedding',
        type: sequelize.normalizeDataType(DataTypes.VECTOR(3)),
      },
    };
    const insert = queryGenerator.insertQuery('items', { embedding: [1, 2, 3] }, attributes);
    const update = queryGenerator.updateQuery(
      'items',
      { embedding: [4, 5, 6] },
      { id: 1 },
      {},
      attributes,
    );

    expect(insert.bind?.sequelize_1).to.deep.equal(Float32Array.from([1, 2, 3]));
    expect(update.bind?.sequelize_1).to.deep.equal(Float32Array.from([4, 5, 6]));
  });

  it('reads VECTOR_INFO when the connected database supports vectors', () => {
    sequelize.setDatabaseVersion('23.26.0');

    expect(queryGenerator.describeTableQuery('items')).to.include('atc.VECTOR_INFO');
  });

  it('does not read VECTOR_INFO when the database version is unknown or too old', () => {
    const unknownVersionSequelize = new Sequelize({ dialect: OracleDialect });
    const oldVersionSequelize = new Sequelize({ dialect: OracleDialect });
    oldVersionSequelize.setDatabaseVersion('21.3.0');

    expect(unknownVersionSequelize.queryGenerator.describeTableQuery('items')).not.to.include(
      'atc.VECTOR_INFO',
    );
    expect(oldVersionSequelize.queryGenerator.describeTableQuery('items')).not.to.include(
      'atc.VECTOR_INFO',
    );
  });
});
