var mongoose = require('mongoose')

var AssetsAddressesSchema = new mongoose.Schema({
  assetId: { type: String, index: true },
  address: String,
  updated: {type: Date, index: true}
})

AssetsAddressesSchema.pre('update', function () {
  this.updated = new Date()
})

AssetsAddressesSchema.index({
    assetId: 1,
    address: 1
  },
  {
    unique: true
  }
)

module.exports = AssetsAddressesSchema
