var mongoose = require('mongoose')

var AssetsUtxosSchema = new mongoose.Schema({
  assetId: { type: String, index: true },
  utxo: String,
  updated: {type: Date, index: true}
})

AssetsUtxosSchema.pre('update', function () {
  this.updated = new Date()
})

AssetsUtxosSchema.index({
    assetId: 1,
    utxo: 1
  },
  {
    unique: true
  }
)

module.exports = AssetsUtxosSchema
