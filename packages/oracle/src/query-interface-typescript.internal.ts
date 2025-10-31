// Copyright (c) 2024, Oracle and/or its affiliates. All rights reserved

import type { FetchDatabaseVersionOptions } from '@sequelize/core';
import { AbstractQueryInterface, QueryTypes } from '@sequelize/core';
import { AbstractQueryInterfaceInternal } from '@sequelize/core/_non-semver-use-at-your-own-risk_/abstract-dialect/query-interface-internal.js';
import type { OracleDialect } from './dialect.js';

export class OracleQueryInterfaceTypescript<
  Dialect extends OracleDialect = OracleDialect,
> extends AbstractQueryInterface<Dialect> {
  readonly #internalQueryInterface: AbstractQueryInterfaceInternal;

  constructor(dialect: Dialect, internalQueryInterface?: AbstractQueryInterfaceInternal) {
    internalQueryInterface ??= new AbstractQueryInterfaceInternal(dialect);

    super(dialect, internalQueryInterface);
    this.#internalQueryInterface = internalQueryInterface;
  }

  async fetchDatabaseVersion(options?: FetchDatabaseVersionOptions): Promise<string> {
    const payload = await this.#internalQueryInterface.fetchDatabaseVersionRaw<{
      VERSION_FULL: string;
    }>(options);

    return payload.VERSION_FULL;
  }

  // async dropAllTables(options?: QiDropAllTablesOptions | undefined): Promise<void> {
  //   const skip = options?.skip || [];
  //   const allTables = await this.listTables(options);
  //   const tableNames = allTables.filter(tableName => !skip.includes(tableName.tableName));

  //   const dropOptions = { ...options };
  //   // enable "cascade" by default if supported by this dialect
  //   if (this.sequelize.dialect.supports.dropTable.cascade && dropOptions.cascade === undefined) {
  //     dropOptions.cascade = true;
  //   }

  //   // Drop all the tables loop to avoid deadlocks and timeouts
  //   for (const tableName of tableNames) {
  //     // eslint-disable-next-line no-await-in-loop
  //     await this.dropTable(tableName, dropOptions);
  //   }
  // }

  async dropAllTables(sequelize: any) {
    // Get all regular (non-nested, non-AQ) tables
    const tables = await this.sequelize.query<{ TABLE_NAME: string }>(
      `
    SELECT TABLE_NAME
    FROM USER_TABLES
    WHERE NESTED = 'NO'
      AND SECONDARY = 'NO'
      AND TABLE_NAME NOT LIKE 'AQ$%'
      AND TABLE_NAME NOT LIKE 'DR$%'
      AND TABLE_NAME NOT LIKE 'NODB_%'
      AND TABLE_NAME NOT LIKE '%_NESTEDTAB'
    ORDER BY TABLE_NAME
    `,
      { type: QueryTypes.SELECT },
    );

    if (tables.length === 0) {
      return;
    }

    // Drop each table one by one (avoid deadlocks)
    for (const { TABLE_NAME } of tables) {
      try {
        sequelize.query(`DROP TABLE "${TABLE_NAME}" CASCADE CONSTRAINTS PURGE`);
      } catch (error: any) {
        console.warn(`Skipped ${TABLE_NAME} — ${error.message}`);
      }
    }
  }
}
