var Sequelize = require('sequelize')

module.exports = {
	HASH: Sequelize.STRING(64),	// even though this could be of type character (n), there is no performance advantage for this type. In fact, its the slowest char type.
	ADDRESS: Sequelize.STRING(35),
	ASSETID: Sequelize.STRING(40)
}