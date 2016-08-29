module.exports = function (mongoose) {
  var AddressesBalancesSchema = new mongoose.Schema({
    address: { type: String, unique: true },
    balance: Number,
    received: Number,
    assets: [{
      assetId: String,
      balance: Number,
      received: Number,
      lockStatus: Boolean,
      divisibility: Number
    }],
    updated: {type: Date, index: true}
  })

  AddressesBalancesSchema.pre('update', function () {
    this.updated = new Date()
  })

  return AddressesBalancesSchema
}
