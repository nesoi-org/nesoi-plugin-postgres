import { migration } from '../migrator';

export default (service: string, tableName: string) => migration({
    service,
    description: `Create a nesoi trash table named ${tableName}`,
    up: async ({ sql }) => {
        await sql.unsafe(`
			CREATE TABLE ${tableName} (
				"id" SERIAL PRIMARY KEY,
                "module" VARCHAR NOT NULL,
                "bucket" VARCHAR NOT NULL,
                "object_id" VARCHAR NOT NULL,
                "object" JSONB NOT NULL,
                "delete_trx_id" VARCHAR NOT NULL,
                "created_by" character(64) ,
				"created_at" timestamp without time zone  NOT NULL,
				"updated_by" character(64) ,
				"updated_at" timestamp without time zone  NOT NULL
			)
		`);
    },
    down: async ({ sql }) => {
        await sql.unsafe(`
			DROP TABLE ${tableName}
		`);
    }
});