/******************************************************************************
 * Dependencies
 *****************************************************************************/
require('dotenv').config();

const GraknClient = require('grakn-client');
const { createLogger, format, transports } = require('winston');
const parse = require('csv-parse/lib/sync');
const fs = require('fs');

/* Used to format dates for Grakn's Java datetime requirement */
const moment = require('moment');

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

  logger.info(`Query to test: match $p isa person; get; offset 0; limit 30;`);
  logger.info(`Inserted person records.`);
}

async function insertBanks(transaction) {
  logger.info(`Inserting banks' data`);

  const data = fs.readFileSync('./data/bank.csv', 'utf-8');
  const records = parse(data, {delimiter: ',', columns: true});
  /*
      Example of a record:
      {
        name: 'N26',
        country: 'Germany',
        headquarters: 'Berlin',
        'free-accounts': 'true',
        'english-customer-service': 'true',
        'english-website': 'true',
        'english-mobile-app': 'true',
        'free-worldwide-withdrawals': 'true',
        'allowed-residents': 'EU residents'
      }
    */
  for (let record of records) {
    const query = `insert $bank isa bank
                , has name "${record['name']}"
                , has country "${record['country']}"
                , has headquarters "${record['headquarters']}"
                , has free-accounts "${record['free-accounts']}"
                , has english-customer-service "${record['english-customer-service']}"
                , has english-website "${record['english-website']}"
                , has english-mobile-app "${record['english-mobile-app']}"
                , has free-worldwide-withdrawals "${record['free-worldwide-withdrawals']}"
                , has allowed-residents "${record['allowed-residents']}"
                ;
    `;
    logger.debug(`Query: ${query}`);
    await transaction.query(query);
  }

  logger.debug(`Committing changes to person data`);
  await transaction.commit();

  logger.info(`Query to test: match $b isa bank; get; offset 0; limit 30;`);
  logger.info(`Inserted bank records.`);
}

async function insertAccounts(transaction) {
  logger.info(`Inserting accounts' data`);

  const data = fs.readFileSync('./data/account.csv', 'utf-8');
  const records = parse(data, {delimiter: ',', columns: true});
  /*
      Example of a record:
      {
        balance: '267.69',
        'account-number': 'DE82444435329779109646',
        'opening-date': '2019-01-16T10:49:31.641721',
        'account-type': 'credit'
      }
    */
  for (let record of records) {
    /* Converting date from input to Java DateTime format for Grakn,
        Expected formats: https://dev.grakn.ai/docs/schema/concepts#define-an-attribute
        Moment.js formats: https://momentjs.com/docs/#/parsing/string-format/
     */
    const datetimeString = moment(record['opening-date']).format('YYYY-MM-DDThh:mm:ss.SSS');   
    const query = `insert $account isa account
                , has balance ${+record['balance']}
                , has account-number "${record['account-number']}"
                , has opening-date ${datetimeString}
                , has account-type "${record['account-type']}"
                ;
    `;
    logger.debug(`Query: ${query}`);
    await transaction.query(query);
  }

  logger.debug(`Committing changes to accounts data`);
  await transaction.commit();

  logger.info(`Query to test: match $a isa account; get; offset 0; limit 30;`);
  logger.info(`Inserted account records.`);
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

  await insertBanks(transaction);

  logger.info(`Closing session...`);
  await session.close();
  logger.info('Closing client...')
  client.close();
})();
