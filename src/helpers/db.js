const { Client } = require('pg')
const { db_host, db_user, db_database, db_password, db_port } = require('../config')

let client

async function connect() {
  client = new Client({
    user: db_user,
    host: db_host,
    database: db_database,
    password: db_password,
    port: db_port
    // ssl: {
    //   rejectUnauthorized: false
    // }
  })

  await client.connect()
}

module.exports = {
  connect,
  query: (text, params) => client.query(text, params)
}
