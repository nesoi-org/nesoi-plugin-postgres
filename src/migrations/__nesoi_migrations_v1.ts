import { migration } from '../migrator';

export default (service: string) => migration({
    service,
    description: 'Create the __nesoi_migrations table',
    up: async ({ sql }) => {
        await sql`
			CREATE TABLE __nesoi_migrations (
				"id" SERIAL PRIMARY KEY,
                "service" VARCHAR NOT NULL,
                "module" VARCHAR NOT NULL,
                "name" VARCHAR NOT NULL,
                "description" VARCHAR,
                "batch" INT4 NOT NULL,
                "timestamp" TIMESTAMP NOT NULL,
                "hash" VARCHAR
			)
		`;
    },
    down: async ({ sql }) => {
        await sql`
			DROP TABLE __nesoi_migrations
		`;
    }
});