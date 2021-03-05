const bodyParser = require('body-parser')
const express = require('express')
const cors = require('cors')

const acsMiddleware = require('./acs-middleware')
const { port } = require('./config')
const db = require('./helpers/db')
const api = require('./api')

const app = express()

app.use(cors())
app.use(bodyParser.json())
app.use(bodyParser.urlencoded({ extended: false }))

const Sentry = require("@sentry/node");
const Tracing = require("@sentry/tracing");

Sentry.init({
  dsn: "https://3b9791a8547949d89f696bffb6ab47ee@o500701.ingest.sentry.io/5580915",

  tracesSampleRate: 1.0,
});

db.connect().then((response) => {
  // Start capturing events
  acsMiddleware.start()

  app.get('/_health', (req, res) => {
    // TODO: check if db connection is ok for example

    res.set('Content-type', 'text/javascript')
    res.send(JSON.stringify({ status: 'Ok' }))
  })

  // API Routes
  app.use('/', api)

  // Error handling
  app.use((err, req, res, next) => {
    console.error(err.message || err, '\n', err.stack)
    res.status(err.statusCode || 500).send({
      message: err.message || 'Unexpected error!'
    })
  })

  app.listen(port, () => {
    console.info(`🚀  Listening at http://localhost:${port}`)
  })
})