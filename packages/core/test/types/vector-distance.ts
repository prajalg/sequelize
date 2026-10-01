import { sql, type VectorMetric } from '@sequelize/core';

const metric: VectorMetric = 'cosine';

sql.vectorDistance(sql.attribute('embedding'), [1, 2, 3], metric);
sql.vectorDistance(sql.attribute('embedding'), new Float32Array([1, 2, 3]), 'euclidean');
sql.vectorDistance(sql.attribute('embedding'), sql.attribute('target'), 'dot');

// @ts-expect-error -- the left operand must be a SQL expression
sql.vectorDistance('embedding', [1, 2, 3], 'cosine');

// @ts-expect-error -- the metric must be part of the VectorMetric union
sql.vectorDistance(sql.attribute('embedding'), [1, 2, 3], 'angular');

// @ts-expect-error -- literal operands must be supported vector values
sql.vectorDistance(sql.attribute('embedding'), 'not-a-vector', 'cosine');
