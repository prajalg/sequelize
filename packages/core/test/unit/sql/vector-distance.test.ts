import { DataTypes, sql } from '@sequelize/core';
import { expect } from 'chai';
import { expectsql, sequelize } from '../../support';

const queryGenerator = sequelize.queryGenerator;

describe('sql.vectorDistance', () => {
  const metricSqlNames = {
    cosine: 'COSINE',
    euclidean: 'EUCLIDEAN',
    euclideanSquared: 'EUCLIDEAN_SQUARED',
    manhattan: 'MANHATTAN',
    dot: 'DOT',
    hamming: 'HAMMING',
    jaccard: 'JACCARD',
  } as const;

  for (const [metric, metricSqlName] of Object.entries(metricSqlNames)) {
    it(`formats the ${metric} metric for the selected dialect`, () => {
      expectsql(
        () =>
          queryGenerator.escape(
            sql.vectorDistance(
              sql.attribute('embedding'),
              sql.attribute('target'),
              metric as keyof typeof metricSqlNames,
            ),
          ),
        {
          default: new Error('Function VectorDistance is not supported'),
          oracle: `VECTOR_DISTANCE("embedding", "target", ${metricSqlName})`,
        },
      );
    });
  }

  it('requires the left operand to be a SQL expression', () => {
    expect(() => sql.vectorDistance('embedding' as never, [1, 2, 3], 'cosine')).to.throw(
      'The left operand of sql.vectorDistance must be a SQL expression. Use sql.attribute(attributeName) to reference a model attribute.',
    );
  });

  it('requires a supported metric', () => {
    expect(() =>
      sql.vectorDistance(sql.attribute('embedding'), [1, 2, 3], 'angular' as never),
    ).to.throw('Invalid vector distance metric: angular');
  });

  const vectorDistanceSupport = sequelize.dialect.supports.vectorDistance;
  const vectorTypeSupport = sequelize.dialect.supports.dataTypes.VECTOR;
  if (
    vectorTypeSupport &&
    vectorTypeSupport.elementTypes.float64 &&
    vectorDistanceSupport &&
    vectorDistanceSupport.metrics.includes('cosine')
  ) {
    it('binds a literal vector through the attribute VECTOR type', () => {
      let boundValue: unknown;

      expectsql(
        () => {
          const Document = sequelize.define(
            'Document',
            {
              embedding: {
                type: DataTypes.VECTOR({ dimensions: 3, elementType: 'float64' }),
                columnName: 'embedding_vector',
              },
            },
            { timestamps: false },
          );

          return queryGenerator.escape(
            sql.vectorDistance(sql.attribute('embedding'), [1, 2, 3], 'cosine'),
            {
              model: Document,
              bindParam(value) {
                boundValue = value;

                return '$vector';
              },
            },
          );
        },
        {
          default: new Error('Function VectorDistance is not supported'),
          oracle: 'VECTOR_DISTANCE("embedding_vector", $vector, COSINE)',
        },
      );

      expect(boundValue).to.deep.equal(Float64Array.from([1, 2, 3]));
    });

    it('escapes a literal vector through the attribute VECTOR type when binds are unavailable', () => {
      expectsql(
        () => {
          const Document = sequelize.define(
            'Document',
            {
              embedding: {
                type: DataTypes.VECTOR({ dimensions: 3, elementType: 'float64' }),
                columnName: 'embedding_vector',
              },
            },
            { timestamps: false },
          );

          return queryGenerator.escape(
            sql.vectorDistance(sql.attribute('embedding'), [1, 2, 3], 'cosine'),
            { model: Document },
          );
        },
        {
          default: new Error('Function VectorDistance is not supported'),
          oracle: 'VECTOR_DISTANCE("embedding_vector", VECTOR(\'[1,2,3]\', 3, FLOAT64), COSINE)',
        },
      );
    });

    it('resolves a mapped associated VECTOR attribute', () => {
      const Collection = sequelize.define('Collection', {}, { timestamps: false });
      const Item = sequelize.define(
        'Item',
        {
          embedding: {
            type: DataTypes.VECTOR({ dimensions: 3, elementType: 'float64' }),
            columnName: 'embedding_vector',
          },
        },
        { timestamps: false },
      );
      Collection.hasMany(Item, { as: 'items', foreignKey: 'collectionId' });

      expectsql(
        () =>
          queryGenerator.escape(
            sql.vectorDistance(sql.attribute('$items.embedding$'), [1, 2, 3], 'cosine'),
            { model: Collection },
          ),
        {
          default: new Error('Function VectorDistance is not supported'),
          oracle:
            'VECTOR_DISTANCE("items"."embedding_vector", VECTOR(\'[1,2,3]\', 3, FLOAT64), COSINE)',
        },
      );
    });
  }

  it('preserves ordinary sql.fn rendering', () => {
    expectsql(
      queryGenerator.escape(
        sql.fn(
          'VECTOR_DISTANCE',
          sql.attribute('first'),
          sql.attribute('second'),
          sql.literal('COSINE'),
        ),
      ),
      {
        default: 'VECTOR_DISTANCE([first], [second], COSINE)',
      },
    );
  });
});
