process.env.DATABASE_URL += '?ssl=true';

module.exports = {
  client: 'pg',
  connection: process.env.DATABASE_URL
};