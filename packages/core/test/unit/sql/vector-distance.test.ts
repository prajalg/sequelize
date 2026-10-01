import { DataTypes, sql } from '@sequelize/core';
import { expect } from 'chai';
import { expectsql, sequelize } from '../../support';

const queryGenerator = sequelize.queryGenerator;

describe('sql.vectorDistance', () => {
  it('renders Oracle vector distance SQL and rejects unsupported dialects', () => {
    expectsql(
      () =>
        queryGenerator.escape(
          sql.vectorDistance(sql.attribute('embedding'), sql.attribute('target'), 'cosine'),
        ),
      {
        default: new Error('Function VectorDistance is not supported'),
        oracle: 'VECTOR_DISTANCE("embedding", "target", COSINE)',
      },
    );
  });

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

  if (sequelize.dialect.supports.vectorDistance) {
    it('maps every metric to its Oracle keyword', () => {
      const expectedMetrics = {
        cosine: 'COSINE',
        euclidean: 'EUCLIDEAN',
        euclideanSquared: 'EUCLIDEAN_SQUARED',
        manhattan: 'MANHATTAN',
        dot: 'DOT',
        hamming: 'HAMMING',
        jaccard: 'JACCARD',
      } as const;

      for (const [metric, oracleMetric] of Object.entries(expectedMetrics)) {
        expect(
          queryGenerator.escape(
            sql.vectorDistance(
              sql.attribute('embedding'),
              sql.attribute('target'),
              metric as keyof typeof expectedMetrics,
            ),
          ),
        ).to.equal(`VECTOR_DISTANCE("embedding", "target", ${oracleMetric})`);
      }
    });

    it('binds a literal vector through the attribute VECTOR type', () => {
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
      let boundValue: unknown;

      const result = queryGenerator.escape(
        sql.vectorDistance(sql.attribute('embedding'), [1, 2, 3], 'cosine'),
        {
          model: Document,
          bindParam(value) {
            boundValue = value;

            return '$vector';
          },
        },
      );

      expect(result).to.equal('VECTOR_DISTANCE("embedding_vector", $vector, COSINE)');
      expect(boundValue).to.deep.equal(Float64Array.from([1, 2, 3]));
    });

    it('preserves ordinary sql.fn rendering', () => {
      expect(
        queryGenerator.escape(
          sql.fn(
            'VECTOR_DISTANCE',
            sql.attribute('first'),
            sql.attribute('second'),
            sql.literal('COSINE'),
          ),
        ),
      ).to.equal('VECTOR_DISTANCE("first", "second", COSINE)');
    });
  }
});
