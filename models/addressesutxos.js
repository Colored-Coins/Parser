module.exports = function (mongoose) {
  var AddressesUtxosSchema = new mongoose.Schema({
    address: { type: String, index: true },
    utxo: String,
    updated: {type: Date, index: true}
  })

  AddressesUtxosSchema.pre('update', function () {
    this.updated = new Date()
  })

  AddressesUtxosSchema.index({
      address: 1,
      utxo: 1
    },
    {
      unique: true
    }
  )
  return AddressesUtxosSchema
}
