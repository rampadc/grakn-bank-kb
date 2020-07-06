require('dotenv').config();

const GraknClient = require('grakn-client');
const { createLogger, format, transports } = require('winston');

/**
 * Create logger
 */
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

/**
 * Main code
 */
(async() => {
  const client = new GraknClient(`${process.env.HOST}:${process.env.PORT}`);
  const session = await client.session(process.env.KEYSPACE);
  
  logger.info(`Connected to grakn server at ${process.env.HOST}:${process.env.PORT} using keyspace ${process.env.KEYSPACE}`);

  logger.info(`Closing session...`);
  await session.close();
  logger.info('Closing client...')
  client.close();
})();
