// Copyright 2024 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { toBuffer } from "./arrow";
import { Connection as _NativeConnection } from "./native";
import { Table } from "./table";
import { Table as ArrowTable } from "apache-arrow";

/**
 * A LanceDB Connection that allows you to open tables and create new ones.
 *
 * Connection could be local against filesystem or remote against a server.
 */
export class Connection {
  readonly inner: _NativeConnection;

  constructor(inner: _NativeConnection) {
    this.inner = inner;
  }

  /** List all the table names in this database. */
  async tableNames(): Promise<string[]> {
    return this.inner.tableNames();
  }

  /**
   * Open a table in the database.
   *
   * @param name The name of the table.
   * @param embeddings An embedding function to use on this table
   */
  async openTable(name: string): Promise<Table> {
    const innerTable = await this.inner.openTable(name);
    return new Table(innerTable);
  }

  /**
   * Creates a new Table and initialize it with new data.
   *
   * @param {string} name - The name of the table.
   * @param data - Non-empty Array of Records to be inserted into the table
   */
  async createTable(
    name: string,
    data: Record<string, unknown>[] | ArrowTable
  ): Promise<Table> {
    const buf = toBuffer(data);
    const innerTable = await this.inner.createTable(name, buf);
    return new Table(innerTable);
  }

  /**
   * Drop an existing table.
   * @param name The name of the table to drop.
   */
  async dropTable(name: string): Promise<void> {
    return this.inner.dropTable(name);
  }
}