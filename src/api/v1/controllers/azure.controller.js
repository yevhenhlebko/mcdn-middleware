const { Client } = require('azure-iothub')
const { Message } = require('azure-iot-common')
const { connectionString } = require('../../../config')

function receiveFeedback(err, receiver) {
  if (err) {
    console.log(err)

    return
  }

  receiver.on('message', (msg) => {
    console.log('Feedback message:')
    console.log(msg.getData().toString('utf-8'))
  })
}

module.exports = {
  sendMessage: (req, res) => {
    try {
      if (!connectionString) {
        console.log('Please set the connectionString environment variable.')

        return res.status(400).json({
          message: 'Connection string required!'
        })
      }

      if (!req.body.targetDevice) {
        console.log('Please give pass a target device id as argument to the script')
        
        return res.status(400).json({
          message: 'Target device required!'
        })
      }

      const serviceClient = Client.fromConnectionString(connectionString)

      serviceClient.open(function (err) {
        if (err) {
          console.error('Could not connect: ' + err.message)

          return res.status(400).json({
            message: 'Could not connect'
          })
        } else {
          serviceClient.getFeedbackReceiver(receiveFeedback)
          const message = new Message(JSON.stringify(req.body.requestJson))

          message.ack = 'full'
          message.messageId = 'My Message ID'

          serviceClient.send(req.body.targetDevice, message, function (err) {
            if (err) {
              console.error(err.toString())

              return res.status(400).json({
                message: err.toString()
              })
            } else {
              console.log('sent c2d message')
              
              return res.status(200).json({
                message: 'sent c2d message'
              })
            }
          })
        }
      })
    } catch (err) {
      console.log(err)

      return res.status(400).json({
        error: 'Error'
      })
    }
  }
}