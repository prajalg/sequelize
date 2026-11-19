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
    for (const tableAs of [undefined, 'root_alias']) {
      it(`qualifies vector operands without changing ordinary ORDER BY expressions (${tableAs ?? 'model alias'})`, () => {
        const Item = sequelize.define(
          'OrderedItem',
          {
            name: { type: DataTypes.STRING, columnName: 'name_column' },
            embedding: { type: DataTypes.VECTOR(3), columnName: 'embedding_vector' },
            comparison: { type: DataTypes.VECTOR(3), columnName: 'comparison_vector' },
          },
          { timestamps: false },
        );
        const alias = tableAs ?? Item.name;
        const options = {
          model: Item,
          tableAs,
          attributes: ['id'],
          order: [
            sql.fn('LOWER', sql.attribute('name')),
            sql.fn(
              'ABS',
              sql.vectorDistance(
                sql.cast(sql.attribute('embedding'), DataTypes.VECTOR(3)),
                sql.attribute('comparison'),
                'cosine',
              ),
            ),
          ],
        };

        expectsql(() => queryGenerator.selectQuery(Item.table, options, Item), {
          default: new Error('Function VectorDistance is not supported'),
          oracle: `SELECT "id" FROM "OrderedItems" "${alias}" ORDER BY LOWER("name_column"), ABS(VECTOR_DISTANCE(CAST("${alias}"."embedding_vector" AS VECTOR(3, FLOAT32)), "${alias}"."comparison_vector", COSINE));`,
        });
      });
    }

    for (const aliased of [false, true]) {
      for (const minifyAliases of [false, true]) {
        it(`resolves ${aliased ? 'aliased' : 'bare'} Attribute projections in a pagination subquery (minifyAliases: ${minifyAliases})`, () => {
          const Item = sequelize.define(
            'ProjectedItem',
            {
              embedding: { type: DataTypes.VECTOR(3), columnName: 'embedding_vector' },
            },
            { timestamps: false },
          );
          const projection = aliased
            ? ([sql.attribute('embedding'), 'selectedEmbedding'] as const)
            : sql.attribute('embedding');
          const outputName = aliased
            ? minifyAliases
              ? '_0'
              : 'selectedEmbedding'
            : 'embedding_vector';
          const selectedSql = `"embedding_vector"${aliased ? ` AS "${outputName}"` : ''}`;

          expectsql(
            () =>
              queryGenerator.selectQuery(
                Item.table,
                {
                  model: Item,
                  // @ts-expect-error -- Bare Attribute projections work at runtime but are not typed.
                  attributes: ['id', projection],
                  subQuery: true,
                  minifyAliases,
                  limit: 1,
                  order: [
                    sql.vectorDistance(
                      sql.attribute('embedding'),
                      sql.attribute('embedding'),
                      'cosine',
                    ),
                  ],
                },
                Item,
              ),
            {
              default: new Error('Function VectorDistance is not supported'),
              oracle: `SELECT "ProjectedItem".* FROM (SELECT "id", ${selectedSql} FROM "ProjectedItems" "ProjectedItem" ORDER BY VECTOR_DISTANCE("ProjectedItem"."embedding_vector", "ProjectedItem"."embedding_vector", COSINE) OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY) "ProjectedItem" ORDER BY VECTOR_DISTANCE("ProjectedItem"."${outputName}", "ProjectedItem"."${outputName}", COSINE);`,
            },
          );
        });
      }
    }

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
          oracle:
            'VECTOR_DISTANCE("embedding_vector", TO_VECTOR(JSON_ARRAY(1D,2D,3D RETURNING CLOB), 3, FLOAT64), COSINE)',
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
            'VECTOR_DISTANCE("items"."embedding_vector", TO_VECTOR(JSON_ARRAY(1D,2D,3D RETURNING CLOB), 3, FLOAT64), COSINE)',
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
