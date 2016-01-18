module.exports = function (mongoose, properties) {
  var BlocksSchema = new mongoose.Schema({
    hash: { type: String, index: { unique: true } },
    previousblockhash: String,
    nextblockhash: String,
    height: { type: Number, index: true },
    size: Number,
    version: Number,
    merkleroot: String,
    time: Date,
    nonce: Number,
    bits: String,
    difficulty: Number,
    chainwork: String,
    txsparsed: { type: Boolean, index: true, default: false},
    txinserted: { type: Boolean, index: true, default: false},
    ccparsed: { type: Boolean, index: true, default: false},
    confirmations: Number,
    reward: { type: Number, set: function (v) { return Math.round(v)} },
    totalsent: { type: Number, set: function (v) { return Math.round(v)} },
    fees: { type: Number, set: function (v) { return Math.round(v)} },
    tx: { type: [String], default: [] },
    txlength: Number
  })

  BlocksSchema.index({
    txinserted: 1, txsparsed: 1, height: 1
  })

  var round = function (doc) {
    if (doc.reward) doc.reward = Math.round(doc.reward)
    if (doc.totalsent) doc.totalsent = Math.round(doc.totalsent)
    if (doc.fees) doc.fees = Math.round(doc.fees)
  }

  BlocksSchema.post('find', function (docs) {
    if (docs) {
      docs.forEach(function (doc) {
        if (properties && properties.last_block) {
          doc.confirmations = properties.last_block - doc.height + 1
        }
        round(doc)
      })
    }
  })

  BlocksSchema.post('findOne', function (doc) {
    if (doc && properties && properties.last_block) {
      doc.confirmations = properties.last_block - doc.height + 1
      return doc
    }
    if (doc) {
      round(doc)  
    }
  })

  return BlocksSchema
}
