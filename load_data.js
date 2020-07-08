/******************************************************************************
 * Dependencies
 *****************************************************************************/
require('dotenv').config();

const GraknClient = require('grakn-client');
const { createLogger, format, transports } = require('winston');
const parse = require('csv-parse/lib/sync');
const fs = require('fs');

/******************************************************************************
 * External variables
 *****************************************************************************/
const CLIENT_URI = `${process.env.HOST}:${process.env.PORT}`;
const KEYSPACE = process.env.KEYSPACE;

/******************************************************************************
 * Create logger
 *****************************************************************************/
const logger = createLogger({
  level: 'info',
  format: format.combine(
    format.timestamp({
      format: 'YYYY-MM-DD HH:mm:ss'
    }),
    format.errors({ stack: true }),
    format.splat(),
    format.json()
  ),
  defaultMeta: { service: 'load-data' },
  transports: [
    new transports.Console()
  ]
});

/******************************************************************************
 * Data loading functions
 *****************************************************************************/
async function insertPersons(transaction) {
  logger.info(`Inserting persons' data`);

  const data = fs.readFileSync('./data/person.csv', 'utf-8');
  const records = parse(data, {delimiter: ',', columns: true});
  /*
      Example of a record:
      {
        'first-name': 'Catalina',
        'last-name': 'Sargent',
        gender: 'female',
        'phone-number': '621-620-0394',
        city: 'Frankfurt',
        email: 'catalinasargent@googlemail.com'
      }
    */
  for (let record of records) {
    const query = `insert $person isa person
                , has email "${record['email']}"
                , has first-name "${record['first-name']}"
                , has last-name "${record['last-name']}"
                , has city "${record['city']}"
                , has phone-number "${record['phone-number']}"
                , has gender "${record['gender']}"
                ;
    `;
    logger.debug(`Query: ${query}`);
    await transaction.query(query);
  }

  logger.debug(`Committing changes to person data`);
  await transaction.commit();

  logger.info('Completed inserting person data');
}

/******************************************************************************
 * Main code
 *****************************************************************************/
(async() => {
  const client = new GraknClient(CLIENT_URI);
  const session = await client.session(KEYSPACE);
  
  logger.info(`Connected to grakn server at ${CLIENT_URI} using keyspace ${KEYSPACE}`);

  logger.debug(`Opening a write transaction to perform a write query...`);
  const transaction = await session.transaction().write();

  await insertPersons(transaction);

  logger.info(`Closing session...`);
  await session.close();
  logger.info('Closing client...')
  client.close();
})();
