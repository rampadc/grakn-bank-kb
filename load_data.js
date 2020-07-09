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
 * Utility functions
 *****************************************************************************/
function toGraknDateTime(isoString) {
  return moment(isoString).format('YYYY-MM-DDThh:mm:ss.SSS');   
}

/******************************************************************************
 * Data loading functions
 *****************************************************************************/
async function insertBankTransactionRelations(session) {
  logger.info(`Inserting bank transaction data`);

  logger.info(`Opening a write transaction...`);
  const transaction = await session.transaction().write();

  const data = fs.readFileSync('./data/transaction.csv', 'utf-8');
  const records = parse(data, {delimiter: ',', columns: true});

  for (let record of records) {
    /*
      Example of a record:
      {
        identifier: '9',
        amount: '803.41',
        'execution-date': '2019-12-16T10:49:31.641890',
        reference: 'thanks',
        category: 'transfer',
        'account-of-receiver': 'DE33510125629974889896',
        'account-of-creator': 'DE10985785971549145687'
      }
    */
    const query = `match $account-of-receiver isa account, has account-number "${record['account-of-receiver']}";
                   $account-of-creator isa account, has account-number "${record['account-of-creator']}";
                   insert $transaction(
                     account-of-receiver: $account-of-receiver, 
                     account-of-creator: $account-of-creator
                   ) isa transaction;
                   $transaction has identifier ${+record['identifier']};
                   $transaction has amount ${+record['amount']};
                   $transaction has reference "${record['reference']}";
                   $transaction has category "${record['category']}";
                   $transaction has execution-date ${toGraknDateTime(record['execution-date'])};
                  `;
    logger.debug(`Query: ${query}`);
    await transaction.query(query);
  }

  logger.debug(`Committing changes to bank transaction data`);
  await transaction.commit();

  logger.info(`Inserted bank transaction records.`);
}

async function insertContractRelations(session) {
  logger.info(`Inserting contract data`);

  logger.info(`Opening a write transaction...`);
  const transaction = await session.transaction().write();

  const data = fs.readFileSync('./data/contract.csv', 'utf-8');
  const records = parse(data, {delimiter: ',', columns: true});

  for (let record of records) {
    /* 
      Example of record:
      {
        identifier: '1',
        'sign-date': '2019-01-13T10:49:31.641721',
        provider: 'Commerzbank',
        customer: 'catalinasargent@googlemail.com',
        offer: 'DE82444435329779109646'
      }
    */
    const query = `match $bank isa bank, has name "${record['provider']}";
                   $customer isa person, has email "${record['customer']}";
                   $account isa account, has account-number "${record['offer']}";
                   insert $contract(
                     provider: $bank,
                     customer: $customer,
                     offer: $account
                   ) isa contract;
                   $contract has identifier ${record['identifier']};
                   $contract has sign-date ${toGraknDateTime(record['sign-date'])};
                  `;
    logger.debug(`Query: ${query}`);
    await transaction.query(query);
  }

  logger.debug(`Committing changes to contract data`);
  await transaction.commit();

  logger.info(`Inserted contract records.`);
}

async function insertRepresentationRelations(session) {
  logger.info(`Inserting representation data`);

  logger.info(`Opening a write transaction...`);
  const transaction = await session.transaction().write();

  const data = fs.readFileSync('./data/represented-by.csv', 'utf-8');
  const records = parse(data, {delimiter: ',', columns: true});

  for (let record of records) {
    /*
      Example of record:
      {
        identifier: '1',
        'bank-account': 'DE82444435329779109646',
        'bank-card': '70120805493'
      }
    */
    const query = `match $account isa account, has account-number "${record['bank-account']}";
                   $card isa card, has card-number ${+record['bank-card']};
                   insert $representation(
                     bank-card: $card,
                     bank-account: $account
                   ) isa represented-by;
                   $representation has identifier ${record['identifier']};
                  `;
    logger.debug(`Query: ${query}`);
    await transaction.query(query);
  }

  logger.debug(`Committing changes to representation data`);
  await transaction.commit();

  logger.info(`Inserted representation records.`);
}

async function insertCards(session) {
  logger.info(`Inserting cards' data`);

  logger.info(`Opening a write transaction...`);
  const transaction = await session.transaction().write();

  const data = fs.readFileSync('./data/card.csv', 'utf-8');
  const records = parse(data, {delimiter: ',', columns: true});
  /*
      Example of a record:
      {
        'card-number': '70120805493',
        'name-on-card': 'Catalina Sargent',
        'created-date': '2019-01-17T10:49:31.641721',
        'expiry-date': '2029-01-14T10:49:31.641721'
      }
    */
  for (let record of records) {
    const query = `insert $card isa card
                , has card-number ${+record['card-number']}
                , has name-on-card "${record['name-on-card']}"
                , has created-date ${toGraknDateTime(record['created-date'])}
                , has expiry-date ${toGraknDateTime(record['expiry-date'])}
                ;
    `;
    logger.debug(`Query: ${query}`);
    await transaction.query(query);
  }

  logger.debug(`Committing changes to cards data`);
  await transaction.commit();

  logger.info(`Query to test: match $c isa card; get; offset 0; limit 30;`);
  logger.info(`Inserted card records.`);
}

async function insertPersons(session) {
  logger.info(`Inserting persons' data`);

  logger.info(`Opening a write transaction...`);
  const transaction = await session.transaction().write();

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

async function insertBanks(session) {
  logger.info(`Inserting banks' data`);
  
  logger.info(`Opening a write transaction...`);
  const transaction = await session.transaction().write();

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

  logger.debug(`Committing changes to banks data`);
  await transaction.commit();

  logger.info(`Query to test: match $b isa bank; get; offset 0; limit 30;`);
  logger.info(`Inserted bank records.`);
}

async function insertAccounts(session) {
  logger.info(`Inserting accounts' data`);

  logger.info(`Opening a write transaction...`);
  const transaction = await session.transaction().write();

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
    const datetimeString = toGraknDateTime(record['opening-date']);
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
logger.info('Before running the scripts below, remember to load in the schema.');
logger.info('grakn console -r HOST:PORT -k KEYSPACE --file schema.gql');
(async() => {
  const client = new GraknClient(CLIENT_URI);
  const session = await client.session(KEYSPACE);
  
  logger.info(`Connected to grakn server at ${CLIENT_URI} using keyspace ${KEYSPACE}`);

  logger.debug(`Opening a write transaction to perform a write query...`);
  
  /* Insert entity data */
  await insertPersons(session);
  await insertAccounts(session);
  await insertBanks(session);
  await insertCards(session);
  /* Insert relation data */
  await insertRepresentationRelations(session);
  await insertBankTransactionRelations(session);
  await insertContractRelations(session);

  logger.info(`Closing session...`);
  await session.close();
  logger.info('Closing client...')
  client.close();
})();
