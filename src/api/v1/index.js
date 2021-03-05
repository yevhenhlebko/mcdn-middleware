const express = require('express')
const azureCtrl = require('./controllers/azure.controller')

const router = express.Router()

/*
|---------------------------------------------------------------------
| Public API - Version being used
|---------------------------------------------------------------------
*/
router.get('/', (req, res) => {
  res.send({
    version: '1.0.0'
  })
})

/*
|---------------------------------------------------------------------
| Receive Azure IoT Hub Messages
|---------------------------------------------------------------------
|
| TODO: description of endpoint
|
*/
router.route('/').post(azureCtrl.sendMessage)

module.exports = router
