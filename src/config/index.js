const dotenv = require('dotenv')

dotenv.config()
module.exports = {
  endpoint: process.env.API_URL,
  port: process.env.PORT,
  consumerGroup: process.env.CONSUMER_GROUP,
  connectionString: process.env.CONNECTION_STRING,
  eventHubName: process.env.EVENTHUB_NAME,
  senderString: process.env.IOTHUB_SENDER_CONNECTION_STRING,
  db_host: process.env.DB_HOST,
  db_user: process.env.DB_USER,
  db_database: process.env.DB_DATABASE,
  db_password: process.env.DB_PASSWORD,
  db_port: process.env.DB_PORT,

  pusherAppId: process.env.PUSHER_APP_ID,
  pusherKey: process.env.PUSHER_KEY,
  pusherSecret: process.env.PUSHER_SECRET,
  pusherCluster: process.env.PUSHER_CLUSTER,
  pusherUseTLS: process.env.PUSHER_TLS
}