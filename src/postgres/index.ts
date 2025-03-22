#!/usr/bin/env node

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListResourcesRequestSchema,
  ListToolsRequestSchema,
  ReadResourceRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import pg from "pg";

const server = new Server(
  {
    name: "example-servers/postgres",
    version: "0.1.0",
  },
  {
    capabilities: {
      resources: {},
      tools: {},
    },
  },
);

const args = process.argv.slice(2);
if (args.length === 0) {
  console.error("Please provide a database URL as a command-line argument");
  process.exit(1);
}

const databaseUrl = args[0];

const resourceBaseUrl = new URL(databaseUrl);
resourceBaseUrl.protocol = "postgres:";
resourceBaseUrl.password = "";

const pool = new pg.Pool({
  connectionString: databaseUrl,
});

const SCHEMA_PATH = "schema";

server.setRequestHandler(ListResourcesRequestSchema, async () => {
  const client = await pool.connect();
  try {
    const result = await client.query(
      "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
    );
    return {
      resources: result.rows.map((row) => ({
        uri: new URL(`${row.table_name}/${SCHEMA_PATH}`, resourceBaseUrl).href,
        mimeType: "application/json",
        name: `"${row.table_name}" database schema`,
      })),
    };
  } finally {
    client.release();
  }
});

server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
  const resourceUrl = new URL(request.params.uri);

  const pathComponents = resourceUrl.pathname.split("/");
  const schema = pathComponents.pop();
  const tableName = pathComponents.pop();

  if (schema !== SCHEMA_PATH) {
    throw new Error("Invalid resource URI");
  }

  const client = await pool.connect();
  try {
    const result = await client.query(
      "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1",
      [tableName],
    );

    return {
      contents: [
        {
          uri: request.params.uri,
          mimeType: "application/json",
          text: JSON.stringify(result.rows, null, 2),
        },
      ],
    };
  } finally {
    client.release();
  }
});

server.setRequestHandler(ListToolsRequestSchema, async () => {
  return {
    tools: [
      {
        name: "query",
        description: "Run a SQL query (read or write)",
        inputSchema: {
          type: "object",
          properties: {
            sql: { type: "string" },
          },
        },
      },
      {
        name: "insert",
        description: "Insert data into a table",
        inputSchema: {
          type: "object",
          properties: {
            table: { type: "string" },
            data: { 
              type: "object",
              additionalProperties: true
            },
          },
          required: ["table", "data"],
        },
      },
      {
        name: "update",
        description: "Update data in a table",
        inputSchema: {
          type: "object",
          properties: {
            table: { type: "string" },
            data: { 
              type: "object",
              additionalProperties: true
            },
            where: { 
              type: "object",
              additionalProperties: true
            },
          },
          required: ["table", "data", "where"],
        },
      },
      {
        name: "delete",
        description: "Delete data from a table",
        inputSchema: {
          type: "object",
          properties: {
            table: { type: "string" },
            where: { 
              type: "object",
              additionalProperties: true
            },
          },
          required: ["table", "where"],
        },
      },
    ],
  };
});

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const client = await pool.connect();
  try {
    switch (request.params.name) {
      case "query": {
        const sql = request.params.arguments?.sql as string;
        const result = await client.query(sql);
        return {
          content: [{ type: "text", text: JSON.stringify(result.rows, null, 2) }],
          isError: false,
        };
      }

      case "insert": {
        const { table, data } = request.params.arguments as { table: string; data: Record<string, any> };
        const columns = Object.keys(data);
        const values = Object.values(data);
        const placeholders = values.map((_, i) => `$${i + 1}`).join(", ");
        
        const sql = `INSERT INTO "${table}" (${columns.map(c => `"${c}"`).join(", ")}) VALUES (${placeholders}) RETURNING *`;
        const result = await client.query(sql, values);
        
        return {
          content: [{ type: "text", text: JSON.stringify(result.rows[0], null, 2) }],
          isError: false,
        };
      }

      case "update": {
        const { table, data, where } = request.params.arguments as { 
          table: string; 
          data: Record<string, any>;
          where: Record<string, any>;
        };

        const setColumns = Object.keys(data);
        const setValues = Object.values(data);
        const whereColumns = Object.keys(where);
        const whereValues = Object.values(where);
        
        const setClause = setColumns.map((col, i) => `"${col}" = $${i + 1}`).join(", ");
        const whereClause = whereColumns.map((col, i) => `"${col}" = $${i + 1 + setValues.length}`).join(" AND ");
        
        const sql = `UPDATE "${table}" SET ${setClause} WHERE ${whereClause} RETURNING *`;
        const result = await client.query(sql, [...setValues, ...whereValues]);
        
        return {
          content: [{ type: "text", text: JSON.stringify(result.rows, null, 2) }],
          isError: false,
        };
      }

      case "delete": {
        const { table, where } = request.params.arguments as {
          table: string;
          where: Record<string, any>;
        };

        const whereColumns = Object.keys(where);
        const whereValues = Object.values(where);
        const whereClause = whereColumns.map((col, i) => `"${col}" = $${i + 1}`).join(" AND ");
        
        const sql = `DELETE FROM "${table}" WHERE ${whereClause} RETURNING *`;
        const result = await client.query(sql, whereValues);
        
        return {
          content: [{ type: "text", text: JSON.stringify(result.rows, null, 2) }],
          isError: false,
        };
      }

      default:
        throw new Error(`Unknown tool: ${request.params.name}`);
    }
  } catch (error) {
    return {
      content: [{ type: "text", text: `Error: ${error.message}` }],
      isError: true,
    };
  } finally {
    client.release();
  }
});

async function runServer() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
}

runServer().catch(console.error);