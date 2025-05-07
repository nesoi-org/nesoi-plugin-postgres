import { Space } from 'nesoi/lib/engine/space';
import PostgresExample from './.nesoi/postgres_example';

const Nesoi = new Space<PostgresExample>(__dirname)
    .name('PostgresExample');

export default Nesoi;