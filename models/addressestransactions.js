var mongoose = require('mongoose')

var AddressesTransactionsSchema = new mongoose.Schema({
  address: { type: String, index: true },
  txid: String,
  updated: {type: Date, index: true}
})

AddressesTransactionsSchema.pre('update', function () {
  this.updated = new Date()
})

AddressesTransactionsSchema.index({
    address: 1,
    txid: 1
  },
  {
    unique: true
  }
)

module.exports = AddressesTransactionsSchema
